import pytest

from pm4pyspark.discovery.dfg import cube
from pm4pyspark.log.log import PysparkLog


def test_raises_when_log_is_not_a_pyspark_log(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)
    with pytest.raises(TypeError) as e:
        cube.build(df, cube_dimensions=['col4'])
    e.match("eventlog argument is of type <class 'pyspark.sql.dataframe.DataFrame'> but should be a PysparkLog")


def test_raises_when_cube_dimension_contains_nonexisting_columns(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    with pytest.raises(ValueError) as e:
        cube.build(pslog, cube_dimensions='missing_col')
    e.match(".* which are not valid columns in eventlog")


def test_raises_when_variant_is_not_defined(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    with pytest.raises(ValueError) as e:
        cube.build(pslog, cube_dimensions='col4', variant='unknown_variant')
    e.match("variant should be either `frequency` or `performance`!")


def test_raises_in_performance_mode_with_make_start_end(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    with pytest.raises(NotImplementedError) as e:
        cube.build(pslog, cube_dimensions='col4', make_start_end=True, variant='performance')
    e.match("performance mode is currently not supported with making start and end activities")


def test_raises_when_cube_dimension_is_not_list(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    with pytest.raises(TypeError) as e:
        cube.build(pslog, cube_dimensions=5)
    e.match("argument cube_dimensions is of type <class 'int'> but expects a string or a list")


def test_correct_behavior_multiple_cube_dimensions(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    # log: a=[<a,b,c>, <c,a>], 1=[<a,b,c>, <d,a>]
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = cube.build(pslog, cube_dimensions=['col4', 'col5'])
    assert len(result) == 2
    assert result[('a', 'x')].type == 'frequency'
    assert result[('a', 'x')].start_activities is None
    assert result[('a', 'x')].end_activities is None
    assert result[('a', 'x')].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('c', 'a'): 1}
    assert result[('1', 'x')].type == 'frequency'
    assert result[('1', 'x')].start_activities is None
    assert result[('1', 'x')].end_activities is None
    assert result[('1', 'x')].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('d', 'a'): 1}


def test_correct_behavior_null_cube_dimension(spark, pd_df_cube_with_null_dimension):
    df = spark.createDataFrame(pd_df_cube_with_null_dimension)

    # log: a=[<a,b,c>, <c,a>], 1=[<a,b,c>, <d,a>]
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = cube.build(pslog, cube_dimensions='col4')
    assert len(result) == 3
    assert result['a'].type == 'frequency'
    assert result['a'].start_activities is None
    assert result['a'].end_activities is None
    assert result['a'].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('c', 'a'): 1}
    assert result['1'].type == 'frequency'
    assert result['1'].start_activities is None
    assert result['1'].end_activities is None
    assert result['1'].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('d', 'a'): 1}
    assert result[None].type == 'frequency'
    assert result[None].start_activities is None
    assert result[None].end_activities is None
    assert result[None].dfg == {('a', 'a'): 1}


def test_correctness_with_timestamp_order_no_make_start_end_string_form(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    # log: a=[<a,b,c>, <c,a>], 1=[<a,b,c>, <d,a>]
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = cube.build(pslog, cube_dimensions='col4')
    assert len(result) == 2
    assert result['a'].type == 'frequency'
    assert result['a'].start_activities is None
    assert result['a'].end_activities is None
    assert result['a'].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('c', 'a'): 1}
    assert result['1'].type == 'frequency'
    assert result['1'].start_activities is None
    assert result['1'].end_activities is None
    assert result['1'].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('d', 'a'): 1}

    # log: a=[<a,b>, <a>, <a,b>], 1=[<a,b>, <a>, <a>, <a>]
    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = cube.build(pslog2, cube_dimensions='col4')
    assert len(result2) == 2
    assert result2['a'].type == 'frequency'
    assert result2['a'].start_activities is None
    assert result2['a'].end_activities is None
    assert result2['a'].dfg == {('a', 'b'): 2}
    assert result2['1'].type == 'frequency'
    assert result2['1'].start_activities is None
    assert result2['1'].end_activities is None
    assert result2['1'].dfg == {('a', 'b'): 1}


def test_correctness_with_timestamp_order_no_make_start_end_list_form(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    # log: a=[<a,b,c>, <c,a>], 1=[<a,b,c>, <d,a>]
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = cube.build(pslog, cube_dimensions=['col4'])
    assert len(result) == 2
    assert result['a'].type == 'frequency'
    assert result['a'].start_activities is None
    assert result['a'].end_activities is None
    assert result['a'].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('c', 'a'): 1}
    assert result['1'].type == 'frequency'
    assert result['1'].start_activities is None
    assert result['1'].end_activities is None
    assert result['1'].dfg == {('a', 'b'): 1, ('b', 'c'): 1, ('d', 'a'): 1}

    # log: a=[<a,b>, <a>, <a,b>], 1=[<a,b>, <a>, <a>, <a>]
    pslog2 = PysparkLog(df, 'col2', 'col1', 'col3')
    result2 = cube.build(pslog2, cube_dimensions=['col4'])
    assert len(result2) == 2
    assert result2['a'].type == 'frequency'
    assert result2['a'].start_activities is None
    assert result2['a'].end_activities is None
    assert result2['a'].dfg == {('a', 'b'): 2}
    assert result2['1'].type == 'frequency'
    assert result2['1'].start_activities is None
    assert result2['1'].end_activities is None
    assert result2['1'].dfg == {('a', 'b'): 1}


def test_correctness_performance_variant_no_make_start_end(spark, pd_df_cube):
    df = spark.createDataFrame(pd_df_cube)

    pslog = PysparkLog(df, 'col1', 'col2', 'col3')
    result = cube.build(pslog, cube_dimensions='col4', variant='performance', date_format='s')
    assert len(result) == 2
    assert result['a'].type == 'performance'
    assert result['a'].start_activities is None
    assert result['a'].end_activities is None
    desired_dfg_a = {('a', 'b'): pytest.approx(1/(60*60*24), 0.01),
                     ('b', 'c'): pytest.approx(1/(60*60*24), 0.01),
                     ('c', 'a'): pytest.approx(46/(60*60*24), 0.01)}
    assert result['a'].dfg == desired_dfg_a
    assert result['1'].type == 'performance'
    assert result['1'].start_activities is None
    assert result['1'].end_activities is None
    desired_dfg_1 = {('a', 'b'): pytest.approx(1/(60*60*24), 0.01),
                     ('b', 'c'): pytest.approx(1/(60*60*24), 0.01),
                     ('d', 'a'): pytest.approx(46/(60*60*24), 0.01)}
    assert result['1'].dfg == desired_dfg_1
