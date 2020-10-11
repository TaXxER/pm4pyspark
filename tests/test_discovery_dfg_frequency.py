import pytest

from pm4pyspark.discovery.dfg import frequency
from pm4pyspark.log.log import PysparkLog

TYPE = 'frequency'


def test_raises_when_log_is_not_a_pyspark_log(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(TypeError) as e:
        frequency.build(df)
    e.match("eventlog argument is of type <class 'pyspark.sql.dataframe.DataFrame'> but should be a PysparkLog")


# log1: [<a,b,c>, <c,a>]
# log2: [<a,b>, <a>, <a,b>]
def test_correctness_with_timestamp_order_no_make_start_end(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_dfg = {('a', 'b'): 1, ('b', 'c'): 1, ('c', 'a'): 1}

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = frequency.build(pslog, make_start_end=False)
    assert result.type == TYPE
    assert result.start_activities is None
    assert result.end_activities is None
    assert result.dfg == desired_dfg

    desired_dfg2 = {('a', 'b'): 2}

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = frequency.build(pslog2, make_start_end=False)
    assert result2.type == TYPE
    assert result2.start_activities is None
    assert result2.end_activities is None
    assert result2.dfg == desired_dfg2


# log1: [<a,b,c>, <a,c>]
# log2: [<a,b>, <a>, <a,b>]
def test_correctness_with_row_order_no_make_start_end(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_dfg = {('a', 'b'): 1, ('b', 'c'): 1, ('a', 'c'): 1}

    pslog = PysparkLog(df, 'col1', 'col2')
    result = frequency.build(pslog, make_start_end=False)
    assert result.type == TYPE
    assert result.start_activities is None
    assert result.end_activities is None
    assert result.dfg == desired_dfg

    desired_dfg2 = {('a', 'b'): 2}

    pslog2 = PysparkLog(df, 'col2', 'col1')
    result2 = frequency.build(pslog2, make_start_end=False)
    assert result2.type == TYPE
    assert result2.start_activities is None
    assert result2.end_activities is None
    assert result2.dfg == desired_dfg2


# log1: [<a,b,c>, <c,a>]
# log2: [<a,b>, <a>, <a,b>]
def test_correctness_with_timestamp_order_make_start_end(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_start_activities = {'a': 1, 'c': 1}
    desired_end_activities = {'a': 1, 'c': 1}
    desired_dfg = {('a', 'b'): 1, ('b', 'c'): 1, ('c', 'a'): 1}

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = frequency.build(pslog, make_start_end=True)
    assert result.type == TYPE
    assert result.start_activities == desired_start_activities
    assert result.end_activities == desired_end_activities
    assert result.dfg == desired_dfg

    desired_start_activities2 = {'a': 3}
    desired_end_activities2 = {'a': 1, 'b': 2}
    desired_dfg2 = {('a', 'b'): 2}

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = frequency.build(pslog2, make_start_end=True)
    assert result2.type == TYPE
    assert result2.start_activities == desired_start_activities2
    assert result2.end_activities == desired_end_activities2
    assert result2.dfg == desired_dfg2


# log1: [<a,b,c>, <a,c>]
# log2: [<a,b>, <a>, <a,b>]
def test_correctness_with_row_order_make_start_end(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_start_activities = {'a': 2}
    desired_end_activities = {'c': 2}
    desired_dfg = {('a', 'b'): 1, ('b', 'c'): 1, ('a', 'c'): 1}

    pslog = PysparkLog(df, 'col1', 'col2')
    result = frequency.build(pslog, make_start_end=True)
    assert result.type == TYPE
    assert result.start_activities == desired_start_activities
    assert result.end_activities == desired_end_activities
    assert result.dfg == desired_dfg

    desired_start_activities2 = {'a': 3}
    desired_end_activities2 = {'a': 1, 'b': 2}
    desired_dfg2 = {('a', 'b'): 2}

    pslog2 = PysparkLog(df, 'col2', 'col1')
    result2 = frequency.build(pslog2, make_start_end=True)
    assert result2.type == TYPE
    assert result2.start_activities == desired_start_activities2
    assert result2.end_activities == desired_end_activities2
    assert result2.dfg == desired_dfg2


# log1: [<a,b,c>, <c,a>]
# log2: [<a,b>, <a>, <a,b>]
def test_correctness_with_timestamp_order_window_size_two(spark, pd_df):
    df = spark.createDataFrame(pd_df)

    desired_dfg = {('a', 'c'): 1}

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = frequency.build(pslog, pm4py_params={'window': 2})
    assert result.type == TYPE
    assert result.dfg == desired_dfg

    desired_dfg2 = {}

    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = frequency.build(pslog2, pm4py_params={'window': 2})
    assert result2.type == TYPE
    assert result2.dfg == desired_dfg2
