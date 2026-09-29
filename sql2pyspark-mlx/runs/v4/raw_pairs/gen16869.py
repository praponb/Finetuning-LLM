from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region", "city").orderBy(F.col("prev_sqft").desc_nulls_last())
w5 = Window.orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("quarter_start").orderBy(F.col("singapore_count").desc())

base = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-12-31")))
    .withColumn("city_region", F.concat("city", F.lit("/"), "region"))
    .withColumn("region_clean", F.coalesce("region", F.lit("NONE")))
)

with_windows = (
    base
    .withColumn("prev_sqft", F.lag("sqft", 1, 0).over(w))
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w2))
)

enriched = (
    with_windows
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w3))
    .withColumn("prev_sqft_cume_dist", F.cume_dist().over(w4))
    .withColumn("prev_sqft_avg_6", F.avg("prev_sqft").over(w5))
)

grouped = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("prev_sqft", 0.5).alias("prev_sqft_p50"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .withColumn("singapore_count_percent_rank", F.percent_rank().over(w6))
)

result = (
    grouped
    .select(
        "quarter_start",
        "activity_month",
        "city_list",
        "prev_sqft_p50",
        "sqft_bit_and",
        "singapore_count_percent_rank",
    )
    .orderBy(F.col("prev_sqft_p50").desc())
)
