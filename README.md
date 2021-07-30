Task: implementing a `batch processing` job as part of a data ETL pipeline. 

- Subtasks:
  - Parsing the input file
  - 70/30 Partition of the data into two splits - namely *train* and *eval* for each class
Input: Bounded data for Beam 

## Instructions to execute
- Please generate the data from slightly changed generate.py file (cmd: `python generate_data.py`)
- run command: `python data_split_script.py` which saves output folders (train and eval) to a folder result.

## Third party library
Distributed computing framework: Apache Beam
It is handy for batch data processing (serves our purpose here) pipelines and job scheduling. The task involves designing a pipeline and executing on a runner. The execution of a pipeline on many workers handled by the Beam automatically on distributed computing frameworks like Spark. Beam has flexibility for different SDKs, designing pipelines, custom transformations, execution on different runners, and much more.  

   
###### How would you go about deploying this batch job on the cluster? 
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
- Independent of the size of the input data file, the number of datapoints per class in the data.

## Courtesy

- [Apache Beam documentation](https://beam.apache.org)
- [Writeup about hands on Apache Beam](https://towardsdatascience.com/hands-on-apache-beam-building-data-pipelines-in-python-6548898b66a5)
- [Simpler Python pipeline with schemas and different transforms](https://www.youtube.com/watch?v=zx4p-UNSmrA&t=336s)
- [stackoverflow](stackoverflow.com/)
- [Docker and Docker compose](https://0x0ece.medium.com/a-quick-demo-of-apache-beam-with-docker-da98b99a502a) 
