from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sqft", "id")
w_grp = Window.orderBy(F.col("running_avg_sqft").desc_nulls_last())
w4 = Window.orderBy(F.col("running_count_sqft").desc(), "id")
w5 = Window.orderBy("running_count_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("running_sum_sqft").desc(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-07-26").cast("date")) & (F.col("city") != "Singapore"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.upper("city").alias("city_upper"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("sqft") == 15529)
    .select(
        "id",
        "sqft",
        "city_upper",
        "region_filled",
        F.avg("sqft").over(w).alias("running_avg_sqft"),
        F.count("sqft").over(w).alias("running_count_sqft"),
        F.sum("sqft").over(w).alias("running_sum_sqft"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w2).alias("sqft_bucket"),
        F.rank().over(w_grp).alias("running_avg_sqft_rank"),
    )
)

metrics_stores = (
    with_windows
    .select(
        "id",
        "region_filled",
        "running_avg_sqft",
        "running_count_sqft",
        "sqft_bucket",
        F.row_number().over(w4).alias("event_seq"),
        F.sum("running_count_sqft").over(w5).alias("running_running_count_sqft"),
        F.lead("running_avg_sqft_rank", 2).over(w2).alias("next_running_avg_sqft_rank"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

result = (
    metrics_stores
    .cube(
        F.when(F.col("sqft_bucket") >= 7, "LARGE").otherwise("SMALL").alias("sqft_bucket_tier"),
    )
    .agg(
        F.percentile_approx("next_running_avg_sqft_rank", [0.25, 0.5, 0.75]).alias("next_running_avg_sqft_rank_quartiles"),
        F.percentile_approx("sqft_bucket", 0.5).alias("sqft_bucket_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
