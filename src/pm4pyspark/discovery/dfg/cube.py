from collections import namedtuple
from typing import (Any, Callable, List, Mapping, Optional, Sequence, Text,
                    Union)

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F

from pm4pyspark.discovery.dfg import frequency, performance
from pm4pyspark.log.log import PysparkLog

CubeParams = namedtuple("CubeParams", ["cube_dimensions", "make_start_end", "variant", "agg_func", "date_format",
                                       "pm4py_params"])


def build(eventlog: PysparkLog,
          cube_dimensions: Union[Text, Sequence[Text]],
          make_start_end: bool = False,
          variant: Text = 'frequency',
          agg_func: Callable[[Union[Text, Column]], Column] = F.avg,
          date_format: Text = 'yyyy-MM-dd HH:mm:ss',
          pm4py_params: Optional[Mapping] = None):
    """
    Creates either a frequency DFG or a performance DFG for every unique value of the columns provided
    in cube_dimensions. The DFG that represents a given value of of cube_dimension is the DFG that is
    calculated on all the rows where cube_dimension has that value. The result is returned in the form of
    a dict with as keys the cube_dimension values and as values the corresponding DFGs.

    Parameters
    ----------
    eventlog
        The log in the form of a pm4pyspark.log.PysparkLog.
    cube_dimensions
        A column of the log that contains values of categorical type. One DFG is generated per unique
        value of these columns.
    make_start_end
        A boolean parameter that indicates whether or not to additionally create and return:
            - a dict with counts of the start activities
            - a dict with counts of the end activities
        These respectively are equivalent to what can be obtained with
        log.get_start_activities and log.get_end_activities.
        Calling this method with `make_start_end=True` is computationally more efficient than
        calling it with `make_start_end=False` and running log.get_start_activities and
        log.get_end_activities separately.
    variant
        A string value of either 'frequency' or 'performance', indicating whether to run
        pm4pyspark.discovery.dfg.frequency or pm4pyspark.discovery.dfg.performance.
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
    if not isinstance(cube_dimensions, list):
        if isinstance(cube_dimensions, str):
            cube_dimensions = [cube_dimensions]
        else:
            raise TypeError('argument cube_dimensions is of type {} but expects a string or a list'
                            .format(type(cube_dimensions)))
    if variant == 'performance' and make_start_end:
        raise NotImplementedError('performance mode is currently not supported with making start and end activities')
    invalid_dimensions = [item for item in cube_dimensions if item not in eventlog.data_frame.columns]
    if invalid_dimensions:
        raise ValueError(
            'cube_dimensions contains {}, which are not valid columns in eventlog'.format(invalid_dimensions))

    dimension_value_combinations = _get_dimension_value_combinations(cube_dimensions, eventlog)
    params = CubeParams(cube_dimensions, make_start_end, variant, agg_func, date_format, pm4py_params)
    result = {}
    for row in dimension_value_combinations:
        if len(row) == 1:
            key = row[0]
        else:
            key = (tuple(row[dimension] for dimension in cube_dimensions))
        result[key] = _process_dimension_value_combination(row, eventlog, params)
    return result


def _process_dimension_value_combination(row: List[Row],
                                         eventlog: PysparkLog,
                                         params: CubeParams):
    assert isinstance(params, CubeParams), 'argument params must be of type CubeParams but is {}'.format(type(params))
    filtered_data = _filter_on_dimensions(eventlog.data_frame, params.cube_dimensions, row)
    filtered_pyspark_log = eventlog.copy_with_new_data(filtered_data)

    if params.variant == 'frequency':
        result = frequency.build(filtered_pyspark_log,
                                 make_start_end=params.make_start_end,
                                 pm4py_params=params.pm4py_params)
    elif params.variant == 'performance':
        result = performance.build(filtered_pyspark_log,
                                   agg_func=params.agg_func,
                                   date_format=params.date_format,
                                   pm4py_params=params.pm4py_params)
    else:
        raise ValueError('variant should be either `frequency` or `performance`!')
    return result


def _filter_on_dimensions(filtered_data: DataFrame,
                          cube_dimensions: Union[Text, Sequence[Text]],
                          row: List[Row]):
    for dimension in cube_dimensions:
        filtered_data = _filter_on_dimension(filtered_data, dimension, row[dimension])
    return filtered_data


def _filter_on_dimension(filtered_data: DataFrame,
                         dimension: Text,
                         value: Optional[Any]):
    if value is None:
        filtered_data = filtered_data.where(F.col(dimension).isNull())
    else:
        filtered_data = filtered_data.where(F.col(dimension) == F.lit(value))
    return filtered_data


def _get_dimension_value_combinations(cube_dimensions: Union[Text, Sequence[Text]],
                                      eventlog: PysparkLog) -> List[Row]:
    dimension_value_combinations = (
        eventlog
        .data_frame
        .groupBy(cube_dimensions)
        .agg(F.count(F.lit(1)).alias('count'))
        .where(F.col('count') > 1)
        .drop('count')
        .collect()
    )
    return dimension_value_combinations
