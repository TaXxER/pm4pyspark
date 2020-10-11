from collections import namedtuple
from typing import Dict, List, Mapping, Optional, Text, Tuple

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F

from pm4pyspark.log.log import PysparkLog
from pm4pyspark.log.operations import get_start_activities

WINDOW = "window"
DEFAULT_WINDOW = 1

ARTIFICIAL_END = 'ARTIFICIAL_END'
NEXT_ACTIVITY_KEY = 'NEXT_ACTIVITY'
TYPE = 'frequency'

DfgResult = namedtuple("DfgResult", ["type", "dfg", "start_activities", "end_activities"])


def build(eventlog: PysparkLog,
          make_start_end: bool = False,
          pm4py_params: bool = None) -> DfgResult:
    """
    Counts the number of directly follows occurrences, i.e. of the form <...a,b...>, in an event log.

    Parameters
    ----------
    eventlog
        Log in the form of a pm4pyspark.log.PysparkLog
    make_start_end
        A boolean parameter that indicates whether or not to additionally create and return:
            - a dict with counts of the start activities
            - a dict with counts of the end activities
        These respectively are equivalent to what can be obtained with
        log.get_start_activities and log.get_end_activities.
        Calling this method with `make_start_end=True` is computationally more efficient than
        calling it with `make_start_end=False` and running log.get_start_activities and
        log.get_end_activities separately.
    pm4py_params
        Possible parameters passed to the algorithms:
            window -> the window size for the DFG. Defaults to 1.

    Returns
    -------
        Returns
    -------
    result
        A pm4pyspark.dfg.frequency.DfgResult object consisting of:
        - result.dfg
            The DFG graph
        - start_activities
            A dict with the counts of how frequently each activity occurs at start of a trace,
            or `None` when `make_start_end=False`
        - end_activities
            A dict with the counts of how frequently each activity occurs at end of a trace, or
            or `None` when `make_start_end=False`
    """
    if not isinstance(eventlog, PysparkLog):
        raise TypeError('eventlog argument is of type {} but should be a PysparkLog'.format(type(eventlog)))
    dfg_df = _build_spark_dataframe(eventlog, make_start_end, pm4py_params)
    dfg, end_activities = _process_spark_output(dfg_df.collect(), eventlog, make_start_end)
    start_activities = _get_start_activities(eventlog, make_start_end)
    return DfgResult(TYPE, dfg, start_activities, end_activities)


def _get_start_activities(eventlog: PysparkLog,
                          make_start_end: bool) -> Dict[str, int]:
    start_activities = None
    if make_start_end:
        start_activities = get_start_activities.apply(eventlog)
    return start_activities


def _build_spark_dataframe(eventlog: PysparkLog,
                           make_start_end: bool,
                           pm4py_params: Optional[Mapping]) -> DataFrame:
    if pm4py_params is None:
        pm4py_params = {}
    window_size = pm4py_params[WINDOW] if WINDOW in pm4py_params else DEFAULT_WINDOW
    relevant_columns = [eventlog.case_glue, eventlog.activity_key]
    spark_df = eventlog.get_and_cache_data()
    if 'timestamp_key' in dir(eventlog):
        sorting_key = eventlog.timestamp_key
    else:
        sorting_key = 'monotonic_row_id'
        spark_df = (
            spark_df
            .withColumn(sorting_key, F.monotonically_increasing_id())
        )
    relevant_columns += [sorting_key]
    window = Window().partitionBy(eventlog.case_glue).orderBy(sorting_key)
    dfg_df = (
        spark_df
        .select(relevant_columns)
        .withColumn(NEXT_ACTIVITY_KEY, F.lead(eventlog.activity_key, window_size).over(window))
    )
    if make_start_end:
        dfg_df = dfg_df.fillna(ARTIFICIAL_END, subset=[NEXT_ACTIVITY_KEY])
    else:
        dfg_df = dfg_df.where(F.col(NEXT_ACTIVITY_KEY).isNotNull())
    dfg_df = (
        dfg_df
        .groupBy([eventlog.activity_key, NEXT_ACTIVITY_KEY])
        .agg(F.count(F.lit(1)).alias('count'))
    )
    return dfg_df


def _process_spark_output(df_collection_result: List[Row],
                          eventlog: PysparkLog,
                          make_start_end: bool) -> Tuple[Dict[Text, int], Dict[Text, int]]:
    dfg = {(row[eventlog.activity_key], row[NEXT_ACTIVITY_KEY]): row['count']
           for row in df_collection_result if row[NEXT_ACTIVITY_KEY] != ARTIFICIAL_END}
    end_activities = None
    if make_start_end:
        end_activities = {row[eventlog.activity_key]: row['count']
                          for row in df_collection_result if row[NEXT_ACTIVITY_KEY] == ARTIFICIAL_END}
    return dfg, end_activities
