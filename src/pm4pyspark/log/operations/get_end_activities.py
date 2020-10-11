from typing import Dict

from pyspark.sql import Window
from pyspark.sql import functions as F

from pm4pyspark.log.log import PysparkLog


def apply(eventlog: PysparkLog) -> Dict[str, int]:
    """
    Counts the number of occurrences of each activity as the last activity in the trace.

    Parameters
    ----------
    eventlog
        Log in the form of a pm4pyspark.log.PysparkLog

    Returns
    -------
    end_activities
        a dict with the counts of how frequently each activity occurs at end of a trace
    """
    if not isinstance(eventlog, PysparkLog):
        raise TypeError('eventlog argument is of type {} but should be a PysparkLog'.format(type(eventlog)))

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
    row_number_key = 'row_number'
    row_nums_df = (
        spark_df
        .select(relevant_columns)
        .withColumn(row_number_key, F.row_number().over(window))
    ).cache()
    max_row_nums = (
        row_nums_df
        .groupBy(eventlog.case_glue)
        .agg(F.max(F.col(row_number_key)).alias(row_number_key))
    )
    end_activities_df = (
        row_nums_df
        .join(
            max_row_nums,
            on=[eventlog.case_glue, row_number_key],
            how='left_semi')
        .groupBy(F.col(eventlog.activity_key))
        .agg(F.count(F.lit(1)).alias('count'))
    )
    row_nums_df.unpersist()
    end_activities = {row[eventlog.activity_key]: row['count'] for row in end_activities_df.collect()}
    return end_activities
