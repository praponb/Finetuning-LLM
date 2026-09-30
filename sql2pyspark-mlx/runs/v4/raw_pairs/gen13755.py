from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("region") == "East")
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "city", "sqft")
)
