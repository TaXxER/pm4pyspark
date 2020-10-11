import pytest

from pm4pyspark.log.log import PysparkLog
from pm4pyspark.log.operations import get_end_activities, get_start_activities


def test_start_activity_raises_when_log_is_not_a_pyspark_log(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(TypeError) as e:
        get_start_activities.apply(df)
    e.match("eventlog argument is of type <class 'pyspark.sql.dataframe.DataFrame'> but should be a PysparkLog")


def test_end_activity_raises_when_log_is_not_a_pyspark_log(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(TypeError) as e:
        get_end_activities.apply(df)
    e.match("eventlog argument is of type <class 'pyspark.sql.dataframe.DataFrame'> but should be a PysparkLog")


# log1: [<a,b,c>, <c,a>]
# log2: [<a,b>, <a>, <a,b>]
def test_start_activity_gives_correct_result_with_timestamp_order(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = get_start_activities.apply(pslog)
    desired_result = {'a': 1, 'c': 1}
    assert result == desired_result

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = get_start_activities.apply(pslog2)
    desired_result2 = {'a': 3}
    assert result2 == desired_result2


# log1: [<a,b,c>, <a,c>]
# log2: [<a,b>, <a>, <a,b>]
def test_start_activity_gives_correct_result_with_row_order(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    pslog = PysparkLog(df, 'col1', 'col2')
    result = get_start_activities.apply(pslog)
    desired_result = {'a': 2}
    assert result == desired_result

    pslog2 = PysparkLog(df, 'col2', 'col1')
    result2 = get_start_activities.apply(pslog2)
    desired_result2 = {'a': 3}
    assert result2 == desired_result2


# log1: [<a,b,c>, <c,a>]
# log2: [<a,b>, <a>, <a,b>]
def test_end_activity_gives_correct_result_with_timestamp_order(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = get_end_activities.apply(pslog)
    desired_result = {'a': 1, 'c': 1}
    assert result == desired_result

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = get_end_activities.apply(pslog2)
    desired_result2 = {'b': 2, 'a': 1}
    assert result2 == desired_result2


# log1: [<a,b,c>, <a,c>]
# log2: [<a,b>, <a>, <a,b>]
def test_end_activity_gives_correct_result_with_row_order(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    pslog = PysparkLog(df, 'col1', 'col2')
    result = get_end_activities.apply(pslog)
    desired_result = {'c': 2}
    assert result == desired_result

    pslog2 = PysparkLog(df, 'col2', 'col1')
    result2 = get_end_activities.apply(pslog2)
    desired_result2 = {'b': 2, 'a': 1}
    assert result2 == desired_result2
