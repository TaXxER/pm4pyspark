from collections import namedtuple
from typing import Mapping, Optional

from pm4py.algo.discovery.inductive.versions.dfg import dfg_based
from pm4py.objects.conversion.process_tree import factory as tree_to_petri

from pm4pyspark.discovery.dfg import frequency
from pm4pyspark.log.log import PysparkLog

AcceptingPetrinet = namedtuple("AcceptingPetrinet", ["petrinet", "initial_marking", "final_marking"])


def build(eventlog: PysparkLog, pm4py_params: Optional[Mapping] = None) -> AcceptingPetrinet:
    """
    Applies the Inductive Miner algorithm to a log obtaining a Petri net along with an initial and final marking.

    Reference:
    Leemans, S. J. J., Fahland, D., & van der Aalst, W. M. P. (2013). Discovering block-structured process models from
    event logs-a constructive approach. Proceedings of the International Conference on Applications and Theory of Petri
    nets and Concurrency (PETRI NETS). pp. 311-329. Springer.

    Parameters
    -----------
    eventlog
        Log in the form of a pm4pyspark.log.PysparkLog
    pm4py_params
        Parameters of the algorithm, including:
            pmutil.constants.PARAMETER_CONSTANT_ACTIVITY_KEY -> attribute of the log to use as activity name
            (default concept:name)
    Returns
    -----------
    apn
        an accepting petri net that consists of three attributes:
        - apn.petrinet
            The Petri net
        - apn.initial_marking
            The initial marking
        - apn.final_marking
            The final marking
    """

    if not isinstance(eventlog, PysparkLog):
        raise TypeError('eventlog argument is of type {} but should be a PysparkLog'.format(type(eventlog)))
    if pm4py_params is None:
        pm4py_params = {}

    dfg_result = frequency.build(eventlog, make_start_end=True, pm4py_params=pm4py_params)
    tree = dfg_based.apply_tree_dfg(dfg_result.dfg,
                                    pm4py_params,
                                    start_activities=dfg_result.start_activities,
                                    end_activities=dfg_result.end_activities)
    net, initial_marking, final_marking = tree_to_petri.apply(tree)
    return AcceptingPetrinet(net, initial_marking, final_marking)
