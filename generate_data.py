# This class generates a random amount of data
# pandas is a dependency `pip install pandas`
# use this to generate a large bunch of data if you wish to benchmark your code

import pandas as pd
import random

classes = ["asset_1", "asset_2"]
max_instances_per_class = 100000
min_instances_per_class = 50000
data = []

for c in classes:
    num_vals = random.randint(min_instances_per_class, max_instances_per_class)
    for i in range(0, num_vals):
        data.append({'classes': c, 'value': random.random()})

df = pd.DataFrame(data)
df = df.sample(frac=1)

df.to_json('data.json', orient='records', lines=True)
