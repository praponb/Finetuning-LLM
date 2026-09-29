from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("east_max_sqft").desc())

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit(" "), "region").alias("city_region"),
        F.upper("store_name").alias("store_name_upper"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    cleaned
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.max(F.when(F.col("region") == "East", F.col("sqft"))).alias("east_max_sqft"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
    )
    .withColumn("east_max_sqft_percent_rank", F.percent_rank().over(w))
)
