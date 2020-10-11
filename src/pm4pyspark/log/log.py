from typing import Optional, Text

from pyspark.sql import DataFrame


class PysparkLog:
    """
    A class that represents an event log that can be processed with PySpark.
    Caching of data in Spark memory is handled automatically when the log is
    used multiple times in the same python session.

    Consists of:
        spark_df
            a spark dataframe that contains the actual data.
        case_glue
            the name of the column in the spark dataframe that contains a
            sequence identifier (also called case glue, case notion, or
            classifier in the process mining field).
        activity_key
            the name of the column in the spark dataframe that contains an
            identifier or name of the step in the sequence (i.e., the activity).
        timestamp_key

    """

    def __init__(self, spark_df: DataFrame, case_glue: Text, activity_key: Text, timestamp_key: Optional[Text] = None):
        if not isinstance(spark_df, DataFrame):
            raise TypeError('Log object not a PySpark dataframe, instead received type {}'.format(type(spark_df)))
        if case_glue not in spark_df.columns:
            raise ValueError('Case column {} is not a column in the log dataframe'.format(case_glue))
        if activity_key not in spark_df.columns:
            raise ValueError('Activity column {} is not a column in the log dataframe'.format(activity_key))
        self.data_frame = spark_df
        self.case_glue = case_glue
        self.activity_key = activity_key
        self._is_cached = False
        if timestamp_key is not None:
            if timestamp_key not in spark_df.columns:
                raise ValueError('Timestamp column {} is not a column in the log dataframe'.format(timestamp_key))
            self.timestamp_key = timestamp_key

    def get_and_cache_data(self):
        if not self._is_cached:
            self.data_frame.cache()
            self._is_cached = True
        return self.data_frame

    def unpersist_data(self):
        self.data_frame.unpersist()
        self._is_cached = False

    def copy_with_new_data(self, new_df: DataFrame) -> 'PysparkLog':
        return PysparkLog(new_df, self.case_glue, self.activity_key, self.timestamp_key) \
            if 'timestamp_key' in dir(self) else PysparkLog(new_df, self.case_glue, self.activity_key)
