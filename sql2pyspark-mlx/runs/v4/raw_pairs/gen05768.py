from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")

latest = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 17534, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("num_rows"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy(F.col("avg_sqft").desc())
)
