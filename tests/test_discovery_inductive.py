import pytest

from pm4pyspark.discovery.inductive import inductive_miner
from pm4pyspark.log.log import PysparkLog


def test_raises_when_log_is_not_a_pyspark_log(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    with pytest.raises(TypeError) as e:
        inductive_miner.build(df)
    e.match("eventlog argument is of type <class 'pyspark.sql.dataframe.DataFrame'> but should be a PysparkLog")


# log1: [<a,b,c>, <c,a>]
def test_resulting_petrinet_has_transitions_matching_activities(spark, pd_df):
    df = spark.createDataFrame(pd_df)
    pslog = PysparkLog(df, 'col1', 'col2', 'col3')

    apn = inductive_miner.build(pslog)
    assert apn.initial_marking is not None
    assert apn.final_marking is not None
    assert apn.petrinet is not None
    expected_labels = {'a', 'b', 'c'}
    labels = {transition.label for transition in apn.petrinet.transitions if transition.label is not None}
    assert labels == expected_labels
