#%%

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


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
    # spark.read.json("./keypress-freq-output.json")
    spark.read.json("/mnt/c/Users/Pablo/keyfreq/data/*.json")
        .withColumn("id", F.monotonically_increasing_id())
        .withColumn("time", (F.from_unixtime(F.lit(0)) + (F.expr("interval 1 second") * F.col("time"))).cast("timestamp"))
        .withColumn("normalized_name", F.upper(F.col("name")))
        # .filter(F.col("_corrupt_record").isNull())
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
keypresses_count = keypresses_count.cache()

## Write keypresses count to csv
keypresses_count.sort("count").toPandas().to_csv("./keypresses_count.csv")

## Show keypresses that are not letters
keypresses_count.sort("count").filter(~F.col("type").isin(["LETTER"])).toPandas()

# %%

# Calcute bigrams from keypresses that are less less than followup_secs separated between them

from pyspark.sql.window import Window
from pyspark.sql import DataFrame

w = Window().partitionBy("input_file").orderBy("time")
followup_secs = 1.0

def bigrams_trf(df: DataFrame) -> DataFrame:
    return (
        df 
        .withColumn("following_key_time", F.lead("time").over(w))
        .withColumn("following_name", F.lead("normalized_name").over(w))
        .withColumn("following_type", F.lead("type").over(w))
        .withColumn("time_delta", (F.col("following_key_time").cast("double") - F.col("time").cast("double")))
        .withColumn("is_followup", (F.col("time_delta") < followup_secs))
        .filter("is_followup")
        .withColumn("bigram", F.array(F.col("normalized_name"), F.col("following_name")))
        .select("time", "following_key_time", "time_delta", "bigram", "type", "following_type")
    )
    

bigrams = keypresses.transform(bigrams_trf)
# Filter shift from bigrams due to it being picked up as part of modified keys or caps
bigrams_cleaned = keypresses.filter(F.col("normalized_name") != "SHIFT").transform(bigrams_trf)

bigrams_count = bigrams.groupby("bigram", "type", "following_type").count().cache()
bigrams_cleaned_count = bigrams_cleaned.groupby("bigram", "type", "following_type").count().cache()

## Write keypresses count to csv
bigrams_count.sort("count").toPandas().to_csv("./bigrams_count.csv")
bigrams_cleaned_count.sort("count").toPandas().to_csv("./bigrams_cleaned_count.csv")

## Show bigrams
bigrams_count.sort("count").toPandas()
bigrams_cleaned_count.sort("count").toPandas()

#%%

keypresses.schema
keypresses.filter("type != 'LETTER'").toPandas()

# %%
