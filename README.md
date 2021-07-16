## ZenML challenge solution

Task: implementing a `batch processing` job as part of a data ETL pipeline. 
  - Parsing the input file
  - 70/30 Partition of the data into two splits - namely *train* and *eval* for each class
Input: Bounded data for Beam 

## Instructions to execute
- Please generate the data from slightly changed generate.py file
- And execute each cell in jupyter notebook

## Third party library
Distributed computing framework: Apache Beam
It is handy for batch data processing(serves our purpose) pipelines and job scheduling. The tasks involve designing pipeline and executing on a runner. The distributed computing on many workers handled by Beam automatically on distributed computing frameworks like Spark. Beam has flexibility of custom transformations. 

## Some answers
1) How would your solution fair if we had an unknown number of classes in our dataset. Would your code still work? If not, what would be your approach to tackle a multi-class split. 
No, my code won't work because I explicitly provied the different classes and transformed them individual dataframe. It would have been possible if I write a custom function which has argument a first column of the dataframe and returns a set of unique classes present in that column, and performs the transformation. 
   
2) How would you go about deploying this batch job on the cluster? A brief, written explanation 
Currently I am testing the Beam pipeline locally since the data file is smaller. In Beam we can chose different runners, eg Spark, Flink. We can setup the runner while designing pipeline using pipeline options for configuring pipeline's execution.  
- Setup in an Option list:

```python
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as p:
```

## Assumptions
- The distribution of datapoints per class is random.
- two possible classes: `asset_1` or `asset_2`. 
- The json element format is { **`classes`**: `asset_1|asset_2`, `value`: `SomeFloat` } instead of { **`class`**: `asset_1|asset_2`, `value`: `SomeFloat` }. My intention was to focus on the task, apologize for that.
- Independent of the size of the input data file, the number of datapoints per class in the data.
- external third-party library : 
- assumption
  - json "class" inflicting with a python keyword "class" so I changed it to "classes".
- Limitation
  - Explicit mentioning of classes unique values

## Limitations
 - The schema should have been the same `class` instead of `classes`
 - The json element in the output file without overhead

## Possible improvements in the codebase and submission
 - Define data processing `class` for abstraction
 - Automating function for extracting unique class_names instead of providing explicitly
 - Performance testing using time module
 - Removing overhead in output files, using appropriate custom `coder`
 - Use of logging module
 - Providing a docker file

## Courtesy

- [Apache Beam documentation](https://beam.apache.org)
- [Writeup about hands on Apache Beam](https://towardsdatascience.com/hands-on-apache-beam-building-data-pipelines-in-python-6548898b66a5)
- [Simpler Python pipeline with schemas and different transforms](https://www.youtube.com/watch?v=zx4p-UNSmrA&t=336s)
- [stackoverflow](stackoverflow.com/)
- [Docker and Docker compose](https://0x0ece.medium.com/a-quick-demo-of-apache-beam-with-docker-da98b99a502a) 
