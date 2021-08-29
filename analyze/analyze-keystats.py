#%%

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


sc = SparkContext('local', 'test')
spark = SparkSession(sc)

#%%

# Lists to classify keypresses

import string
letters = list(string.ascii_letters)
numbers = list(string.digits)
symbols = list(string.punctuation)
modifiers = ["SHIFT", "CTRL", "ALT", "TAB", "LEFT WINDOWS", "RIGHT WINDOWS"]
whitespace = ["SPACE", "BACKSPACE", "DELETE", "ENTER"]

#%%

# Read keypresses extend the columns

keypresses = (
    spark.read.json("./keypress-freq-output.json")
        .withColumn("id", F.monotonically_increasing_id())
        .withColumn("time", (F.from_unixtime(F.lit(0)) + (F.expr("interval 1 second") * F.col("time"))).cast("timestamp"))
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
        .withColumn("input_file", F.input_file_name())
)
keypresses.schema
#%%

# Number of keypresses in the dataset
keypresses.cache().count()

#%%

# Raw keypress count

keypresses_count = keypresses.groupby("normalized_name", "type").count()

## Show keypresses that are not letters
keypresses_count.sort("count").filter(~F.col("type").isin(["LETTER"])).toPandas()

# %%

# Calcute bigrams from keypresses that are less less than followup_secs separated between them

from pyspark.sql.window import Window

w = Window().partitionBy("input_file").orderBy("time")
followup_secs = 1.0

bigrams = (
    keypresses
        .withColumn("following_key_time", F.lead("time").over(w))
        .withColumn("following_name", F.lead("normalized_name").over(w))
        .withColumn("time_delta", (F.col("following_key_time").cast("double") - F.col("time").cast("double")))
        .withColumn("is_followup", (F.col("time_delta") < followup_secs))
        .filter("is_followup")
        .withColumn("bigram", F.array(F.col("normalized_name"), F.col("following_name")))
        .select("time", "following_key_time", "time_delta", "bigram")
)

bigrams.groupby("bigram").count().sort("count").toPandas()

#%%

keypresses.schema
keypresses.filter("type != 'LETTER'").toPandas()

# %%
