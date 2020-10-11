# What's in my sequences?
- Do you have data of sequential nature? 

- Do you feel like you are missing the tools to easily and quickly do an *explorative analysis* on sequential data?

A tool that brings the data exploration capabilities of [__process mining__](https://www.tf-pm.org) to the data volumes that are found at web scale companies.

# Example & demonstration
## On public data

We download and import a public data set from a production process (see [desciption](https://data.4tu.nl/repository/uuid:68726926-5ac5-4fab-b873-ee76ea412399)).

```python
from pm4pyspark.log.log import PysparkLog
from pm4pyspark.discovery.dfg import cube
from pm4py.visualization.dfg import factory as dfg_vis_factory

import pandas as pd
import io
import requests

# loading the data
url = "https://data.4tu.nl/download/uuid:68726926-5ac5-4fab-b873-ee76ea412399/file/?file_path=data%2FProduction_Data.csv"
s = requests.get(url).content
pd_production_data = pd.read_csv(io.StringIO(s.decode('utf-8')))

# Prepare the input data
df_production_data = spark.createDataFrame(pd_production_data)
# parameters in the following order:
# 1) the spark dataframe that you want to explore
# 2) then column name that identifies a sequence
# 3) the column name that gives a name to a step in the sequence
# 4) the column that has a timestamp of the step (optional) 
input_data = PysparkLog(df_production_data, 'Case ID', 'Activity', 'Start Timestamp')

# A separate model will be build for every unique value of the column indicates by the second parameter.
# Instead use pm4pyspark.discovery.dfg.frequency or pm4pyspark.discovery.dfg.performance if you want 
# to make a single graph for your whole dataframe.
result = cube.build(input_data, ['Part Desc.'])

# Visualize it, this yields the two attached images
for key in result.keys():
    dfg_vis_factory.apply(result[key].dfg)
```

