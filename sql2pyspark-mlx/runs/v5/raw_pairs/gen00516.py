from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first(), "id")

deduped = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
