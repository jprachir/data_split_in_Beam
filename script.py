# This class generates a random amount of data
# pandas is a dependency `pip install pandas`
# use this to generate a large bunch of data if you wish to benchmark your code

#setup
import apache_beam as beam
import json
import typing
from apache_beam.dataframe.transforms import DataframeTransform
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection


#ouput folder: where to save files
train_output_filename = "./result/train/data.json"
eval_output_filename = "./result/eval/data.json"


#A Dataframe Transform requires input PCollections to have a schema
class applying_schema(typing.NamedTuple):
    """Represents a Json element with schema (classes and value)."""
    classes: str
    value: float
beam.coders.registry.register_coder(applying_schema, beam.coders.RowCoder)


# splitting Pcollection.
def split_dataset(elem, num_partitions, ratio):
    """Returns splitted pcoll
    Arg1: elem: the element being processed
    Arg2: num_partitions: How many partitions
    Arg3: ratio: ratio of splitting
    Returns: The processed pcoll
    """
    assert num_partitions == len(ratio)
    bucket = sum(map(ord, json.dumps(elem))) % sum(ratio)
    total = 0
    for i, part in enumerate(ratio):
        total += part
        if bucket < total:
            return i
    return len(ratio) - 1


# Create a pipeline.
with beam.Pipeline() as pdf:
    # res is a Pcollection
    res = (pdf
            |"reading json file" >> beam.io.ReadFromText("./data.json")
            |"loading json" >> beam.Map(json.loads)
            |"applying schema" >> beam.Map(lambda x:applying_schema(**x)).with_output_types(applying_schema))
    
    
    # Converting to a beam dataframe
    df = to_dataframe(res)
    
    
    # Two dataframes for each classes
    df1 = df.loc[df['classes']=='asset_1']
    df2 = df.loc[df['classes']=='asset_2']
    
    
    # Converting again to Pcollections to apply Partition (70/30) function
    # For asset_1 class
    train_1,eval_1 = (to_pcollection(df1)
                      | 'Partition1' >> beam.Partition(split_dataset, 2, ratio=[7, 3]))
    # For asset_2 class
    train_2,eval_2 = (to_pcollection(df2)
                      | 'Partition2' >> beam.Partition(split_dataset, 2, ratio=[7, 3]))

    
    # Aggregating 70/30 partition for each class to pcol
    #aggregating train and eval for asset_1 class
    TRAIN = ((train_1,train_2) |"aggregate train" >> beam.Flatten())
    #aggregating train and eval for asset_2 class
    EVAL = ((eval_1,eval_2)|"aggregate eval" >> beam.Flatten())
    
    
    # Writing each pcol to json files
    # Saving TRAIN data for each class to output folder result
    result = (TRAIN|"write to train" >> beam.io.WriteToText(train_output_filename))
    # Saving EVAL data for each class to output folder result
    result = (EVAL|"write to eval" >> beam.io.WriteToText(eval_output_filename))
