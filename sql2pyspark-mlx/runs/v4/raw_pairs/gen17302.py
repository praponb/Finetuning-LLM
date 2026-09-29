from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last(), "id")

latest = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .groupBy("region")
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.max(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_max_sqft"),
    )
    .orderBy(F.col("num_cities").asc_nulls_first())
)
