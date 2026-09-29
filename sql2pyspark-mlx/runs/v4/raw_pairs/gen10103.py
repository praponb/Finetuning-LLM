from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w4 = Window.orderBy(F.col("prev_sqft").asc_nulls_last())
w5 = Window.partitionBy("region").orderBy("sqft_avg_6_prior", "id")
w6 = Window.partitionBy("city").orderBy(F.col("prev_sqft").desc(), "id")
w7 = Window.partitionBy("city", "region")
w8 = Window.partitionBy("city")
w9 = Window.partitionBy("region")
w10 = Window.partitionBy("region").orderBy("sqft_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_stores
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w2))
    .withColumn("sqft_rank", F.rank().over(w3))
)

valid = (
    metrics
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-04-23")))
    .select("*", F.concat_ws("-", "store_name", "city").alias("store_name_city_key"))
)

metrics_stores = (
    valid
    .select(
        "*",
        F.rank().over(w4).alias("prev_sqft_rank"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(5).over(w6).alias("prev_sqft_bucket"),
        F.count("sqft_rank").over(w7).alias("city_count_sqft_rank"),
        F.sum("prev_sqft").over(w8).alias("city_sum_prev_sqft"),
    )
)

scored = (
    metrics_stores
    .select(
        "*",
        F.sum("prev_sqft").over(w9).alias("region_sum_prev_sqft"),
        F.first("sqft_rank", ignorenulls=True).over(w10).alias("first_known_sqft_rank"),
    )
)

result = (
    scored
    .groupBy("region")
    .agg(
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.avg("region_sum_prev_sqft").alias("avg_region_sum_prev_sqft"),
        F.round(F.avg("first_known_sqft_rank"), 2).alias("avg_first_known_sqft_rank"),
    )
    .filter(F.col("avg_first_known_sqft_rank") >= 0)
    .orderBy(F.col("sqft_p90").desc())
)
