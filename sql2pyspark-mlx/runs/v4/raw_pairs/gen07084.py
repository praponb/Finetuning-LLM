from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_last(), "id")

deduped = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sum("sqft").alias("total_sqft"),
        F.count("*").alias("event_count"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
    )
    .orderBy(F.col("distinct_cities").asc_nulls_last())
)
