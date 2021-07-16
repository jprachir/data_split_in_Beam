## ZenML challenge solution

Task: implementing a `batch processing` job as part of a data ETL pipeline. 
Subtasks:
  - Parsing the input file
  - 70/30 Partition of the data into two splits - namely *train* and *eval* for each class
Input: Bounded data for Beam 

## Instructions to execute
- Please generate the data from slightly changed generate.py file (cmd: `python generate_data.py`)
- run command: `python script.py` which saves output folders (train and eval) to a folder result.

## Third party library
Distributed computing framework: Apache Beam
It is handy for batch data processing (serves our purpose here) pipelines and job scheduling. The task involves designing a pipeline and executing on a runner. The execution of a pipeline on many workers handled by the Beam automatically on distributed computing frameworks like Spark. Beam has flexibility for different SDKs, designing pipelines, custom transformations, execution on different runners, and much more.  

## Some answers
1) How would your solution fair if we had an unknown number of classes in our dataset. Would your code still work? If not, what would be your approach to tackle a multi-class split. 
- Well, no, my code will not work because I explicitly provided the different available classes(`asset_1` and `asset_2`) and transformed them into individual dataframe. It would have been easier if I wrote a custom function which has an argument: first column of the dataframe, and returns: a set of unique classes present in that column, and perform the transformation. 
   
2) How would you go about deploying this batch job on the cluster? A brief, written explanation 
Currently, I am testing the Beam pipeline locally (a DirectRunner) since the data file size is small. In Beam, we can choose different runners, eg, Spark, Flink. We can set up the runner while designing pipeline using the pipeline options for configuring the pipeline's execution.  
- Setup in an Option list:

```python
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=PortableRunner",
    "--job_endpoint=localhost:8099",
    "--environment_type=LOOPBACK"
])
with beam.Pipeline(options) as pdf:
```

## Assumptions
- The distribution of datapoints per class is random.
- two possible classes: `asset_1` or `asset_2`. 
- The json element format is { **`classes`**: `asset_1|asset_2`, `value`: `SomeFloat` } instead of { **`class`**: `asset_1|asset_2`, `value`: `SomeFloat` }. My intention was to focus on the task, apologize for that. (json "class" conflicting with a python keyword "class" so I changed it to "classes".)
- Independent of the size of the input data file, the number of datapoints per class in the data.
- The reviewers are aware of beam keywords like Pipeline, PCollection, PTransform and so on, with it's operators (`|`,`>>`)

## Limitations
 - The schema should have been the same `class` instead of `classes`
 - The json element in the output file without overhead

## Possible improvements in the codebase and submission
 - Define data processing `class` for abstraction
 - Automating function for extracting unique class_names instead of providing explicitly
 - Performance testing using time module
 - Removing overhead in output files, using appropriate custom `coder`
 - Use of a logging module
 - Providing a docker file

## Courtesy

- [Apache Beam documentation](https://beam.apache.org)
- [Writeup about hands on Apache Beam](https://towardsdatascience.com/hands-on-apache-beam-building-data-pipelines-in-python-6548898b66a5)
- [Simpler Python pipeline with schemas and different transforms](https://www.youtube.com/watch?v=zx4p-UNSmrA&t=336s)
- [stackoverflow](stackoverflow.com/)
- [Docker and Docker compose](https://0x0ece.medium.com/a-quick-demo-of-apache-beam-with-docker-da98b99a502a) 
