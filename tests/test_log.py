import pytest

from pm4pyspark.log.log import PysparkLog


def test_log_cache_and_unpersist(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    assert not pslog._is_cached
    result_df = pslog.get_and_cache_data()
    assert result_df is pslog.data_frame
    assert pslog._is_cached
    pslog.unpersist_data()
    assert not pslog._is_cached


def test_log_clone_does_not_raise_df_of_correct_type_and_all_columns_exist(spark, pd_df, pd_df_filtered):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    df2 = spark.createDataFrame(pd_df_filtered)
    pslog_copy = pslog.copy_with_new_data(df2)
    assert pslog_copy.data_frame is df2
    assert pslog_copy.data_frame is not df
    assert pslog.data_frame is df
    assert pslog.data_frame is not df2


def test_log_clone_does_raise_when_df_of_incorrect_type(spark, pd_df, pd_df_filtered):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    with pytest.raises(TypeError) as e:
        pslog.copy_with_new_data(pd_df_filtered)
    e.match("Log object not a PySpark dataframe")


def test_log_clone_raises_when_case_glue_col_not_in_df(spark, pd_df, pd_df_filtered):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    df2 = spark.createDataFrame(pd_df_filtered).withColumnRenamed('col1', 'c')
    with pytest.raises(ValueError) as e:
        pslog.copy_with_new_data(df2)
    e.match("Case column col1 is not a column in the log dataframe")


def test_log_clone_raises_when_activity_col_not_in_df(spark, pd_df, pd_df_filtered):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    df2 = spark.createDataFrame(pd_df_filtered).withColumnRenamed('col2', 'c')
    with pytest.raises(ValueError) as e:
        pslog.copy_with_new_data(df2)
    e.match("Activity column col2 is not a column in the log dataframe")


def test_log_clone_raises_when_timestamp_col_not_in_df(spark, pd_df, pd_df_filtered):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    df2 = spark.createDataFrame(pd_df_filtered).withColumnRenamed('col3', 'c')
    with pytest.raises(ValueError) as e:
        pslog.copy_with_new_data(df2)
    e.match("Timestamp column col3 is not a column in the log dataframe")


def test_log_does_not_raise_df_of_correct_type_and_all_columns_exist(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    PysparkLog(df, 'col1', 'col2', 'col3')


def test_log_does_raise_when_df_of_incorrect_type(pd_df):
    with pytest.raises(TypeError) as e:
        PysparkLog(pd_df, 'col1', 'col2', 'col3')
    e.match("Log object not a PySpark dataframe")


def test_log_raises_when_case_glue_col_not_in_df(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(ValueError) as e:
        PysparkLog(df, 'c', 'col2', 'col3')
    e.match("Case column c is not a column in the log dataframe")


def test_log_raises_when_activity_col_not_in_df(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(ValueError) as e:
        PysparkLog(df, 'col1', 'c', 'col3')
    e.match("Activity column c is not a column in the log dataframe")


def test_log_raises_when_timestamp_col_not_in_df(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(ValueError) as e:
        PysparkLog(df, 'col1', 'col2', 'c')
    e.match("Timestamp column c is not a column in the log dataframe")
