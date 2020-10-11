import pytest

from pm4pyspark.discovery.dfg import performance
from pm4pyspark.log.log import PysparkLog

TYPE = 'performance'


def test_raises_when_log_is_not_a_pyspark_log(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(TypeError) as e:
        performance.build(df)
    e.match("eventlog argument is of type <class 'pyspark.sql.dataframe.DataFrame'> but should be a PysparkLog")


def test_raises_when_log_does_not_contain_timestamp_column(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2')
    with pytest.raises(ValueError) as e:
        performance.build(pslog)
    e.match("a performance dfg requires the PysparkLog to have a timestamp column")


# log1: [<a,b,c>, <c,a>46]
# log2: [<a,b>49, <a>, <a,b>]
def test_correctness_with_timestamp_order(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_dfg = {('a', 'b'): pytest.approx(1/(60*60*24), 0.01),
                   ('b', 'c'): pytest.approx(1/(60*60*24), 0.01),
                   ('c', 'a'): pytest.approx(46/(60*60*24), 0.01)}

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = performance.build(pslog, date_format='s')
    assert result.type == TYPE
    assert result.dfg == desired_dfg

    avg_a_to_b_time = ((1+49)/2)/(60*60*24)
    desired_dfg2 = {('a', 'b'): pytest.approx(avg_a_to_b_time, 0.01)}

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = performance.build(pslog2, date_format='s')
    assert result2.type == TYPE
    assert result2.dfg == desired_dfg2


# log1: [<a,b,c>, <c,a>]
# log2: [<a,b>, <a>, <a,b>]
def test_correctness_with_timestamp_order_window_size_two(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_dfg = {('a', 'c'): pytest.approx(2/(60*60*24), 0.01)}

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = performance.build(pslog, date_format='s', pm4py_params={'window': 2})
    assert result.type == TYPE
    assert result.dfg == desired_dfg

    desired_dfg2 = {}

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = performance.build(pslog2, date_format='s', pm4py_params={'window': 2})
    assert result2.type == TYPE
    assert result2.dfg == desired_dfg2
