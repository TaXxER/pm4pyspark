from typing import (Any, Callable, Dict, List, Mapping, Optional, Text, Tuple,
                    Union)

from pyspark.sql import Column, DataFrame, Row, Window
from pyspark.sql import functions as F

from pm4pyspark.discovery.dfg.frequency import DfgResult
from pm4pyspark.log.log import PysparkLog

WINDOW = "window"
DEFAULT_WINDOW = 1

NEXT_ACTIVITY_KEY = 'NEXT_ACTIVITY'
NEXT_TIMESTAMP_KEY = 'NEXT_TIMESTAMP'
NEXT_KEY = 'NEXT_KEY'
TYPE = 'performance'


def build(eventlog: PysparkLog,
          agg_func: Callable[[Union[Text, Column]], Column] = F.avg,
          date_format: Text = 'yyyy-MM-dd HH:mm:ss',
          pm4py_params: Optional[Mapping] = None) -> DfgResult:
    """
    Aggregates the time differences per pair of activities.

    Parameters
    ----------
    eventlog
        The log in the form of a pm4pyspark.log.PysparkLog.
    agg_func
        Only for variant='performance'. The aggregation function used to aggregate the time differences
        between the activity pairs.
    date_format
        Only for variant='performance'. The date format used to parse the timestamp column.
    pm4py_params
        Possible parameters passed to the algorithms:
            window -> the window size for the DFG. Defaults to 1.

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
    if 'timestamp_key' not in dir(eventlog):
        raise ValueError('a performance dfg requires the PysparkLog to have a timestamp column')
    dfg_df = _build_spark_dataframe(eventlog, agg_func, date_format, pm4py_params)
    dfg = _process_spark_output(dfg_df.collect(), eventlog)
    return DfgResult(TYPE, dfg, None, None)


def _build_spark_dataframe(eventlog: PysparkLog,
                           agg_func: Callable[[Union[Text, Column]], Column],
                           date_format: Text,
                           pm4py_params: Optional[Mapping]) -> DataFrame:
    if pm4py_params is None:
        pm4py_params = {}
    window_size = pm4py_params[WINDOW] if WINDOW in pm4py_params else DEFAULT_WINDOW
    eventlog.unpersist_data()
    spark_df = eventlog.get_and_cache_data()

    window = Window().partitionBy(eventlog.case_glue).orderBy(eventlog.timestamp_key)
    dfg_df = (
        spark_df
        .select(eventlog.case_glue, eventlog.activity_key, eventlog.timestamp_key)
        .withColumn(eventlog.timestamp_key, F.to_timestamp(eventlog.timestamp_key, date_format))
        .withColumn(NEXT_KEY, F.lead(
            F.struct(eventlog.timestamp_key, eventlog.activity_key), window_size).over(window))
        .where(F.col(NEXT_KEY).isNotNull())
        .withColumn(NEXT_ACTIVITY_KEY, F.col(NEXT_KEY)[eventlog.activity_key])
        .withColumn(NEXT_TIMESTAMP_KEY, F.col(NEXT_KEY)[eventlog.timestamp_key])
        .withColumn('time_diff_hours',
                    (F.col(NEXT_TIMESTAMP_KEY).cast('long') - F.col(eventlog.timestamp_key).cast('long')) / (60*60*24))
        .groupBy([eventlog.activity_key, NEXT_ACTIVITY_KEY])
        .agg(agg_func(F.col('time_diff_hours')).alias('aggregate'))
    )
    return dfg_df


def _process_spark_output(df_collection_result: List[Row],
                          eventlog: PysparkLog) -> Dict[Tuple[Text, Text], Any]:
    dfg = {(row[eventlog.activity_key], row[NEXT_ACTIVITY_KEY]): row['aggregate'] for row in df_collection_result}
    return dfg
