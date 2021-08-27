#%%

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


sc = SparkContext('local', 'test')
spark = SparkSession(sc)

#%%

import string
letters = list(string.ascii_letters)
numbers = list(string.digits)
symbols = list(string.punctuation)
modifiers = ["SHIFT", "CTRL", "ALT", "TAB", "LEFT WINDOWS", "RIGHT WINDOWS"]
whitespace = ["SPACE", "BACKSPACE", "DELETE", "ENTER"]

#%%

keypresses = (
    spark.read.json("./keypress-freq-output.json")
        .withColumn("time", F.from_unixtime("time"))
        .withColumn("normalized_name", F.upper(F.col("name")))
        .filter(F.col("_corrupt_record").isNull())
        .withColumn("type",
            F.when(F.col("normalized_name").isin(letters), F.lit("LETTER"))
             .when(F.col("normalized_name").isin(numbers), F.lit("NUMBER"))
             .when(F.col("normalized_name").isin(symbols), F.lit("SYMBOL"))
             .when(F.col("normalized_name").isin(modifiers), F.lit("MODIFIER"))
             .when(F.col("normalized_name").isin(whitespace), F.lit("WHITESPACE"))
             .otherwise(F.lit("OTHERS"))
        )
)


#%%

keypresses_count = keypresses.groupby("normalized_name", "type").count()

#%%

keypresses_count.sort("count").filter(~F.col("type").isin(["LETTER"])).toPandas()

# %%

keypresses.schema
keypresses.filter("type is null").toPandas()
# %%
