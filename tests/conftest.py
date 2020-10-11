import pandas as pd
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='package')
def pd_df():
    d = {'col1': ['a', 'a', 'a', 'b', 'b'], 'col2': ['a', 'b', 'c', 'a', 'c'], 'col3': ['1', '2', '3', '50', '4']}
    return pd.DataFrame(data=d)


@pytest.fixture(scope='package')
def pd_df_cube():
    d = {'col1': ['a', 'a', 'a', 'b', 'b', 'a', 'a', 'a', 'b', 'b'],
         'col2': ['a', 'b', 'c', 'a', 'c', 'a', 'b', 'c', 'a', 'd'],
         'col3': ['1', '2', '3', '50', '4', '1', '2', '3', '50', '4'],
         'col4': ['a', 'a', 'a', 'a', 'a', '1', '1', '1', '1', '1'],
         'col5': ['x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x']
         }
    return pd.DataFrame(data=d)


@pytest.fixture(scope='package')
def pd_df_cube_with_null_dimension():
    d = {'col1': ['a', 'a', 'a', 'b', 'b', 'a', 'a', 'a', 'b', 'b', 'a', 'a'],
         'col2': ['a', 'b', 'c', 'a', 'c', 'a', 'b', 'c', 'a', 'd', 'a', 'a'],
         'col3': ['1', '2', '3', '50', '4', '1', '2', '3', '50', '4', '8', '9'],
         'col4': ['a', 'a', 'a', 'a', 'a', '1', '1', '1', '1', '1', None, None]}
    return pd.DataFrame(data=d)


@pytest.fixture(scope='package')
def pd_df_filtered():
    d = {'col1': ['a', 'a', 'a', 'b'], 'col2': ['a', 'b', 'c', 'a'], 'col3': ['1', '2', '3', '50']}
    return pd.DataFrame(data=d)


@pytest.fixture(scope='package')
def spark():
    spark = (
        SparkSession.builder
        .appName('testing')
        .master('local[1]')
        .config("spark.sql.shuffle.partitions", 1)
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    return spark
