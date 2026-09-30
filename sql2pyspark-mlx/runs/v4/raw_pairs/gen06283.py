from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("next_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("cumulative_sqft", "id")
w6 = Window.partitionBy("region").orderBy("sqft", "id")
w7 = Window.orderBy(F.col("sqft").asc_nulls_first())
w8 = Window.partitionBy("region")
w_running = Window.partitionBy("region").orderBy("cumulative_sqft_bucket", "id").rowsBetween(-6, -1)
w10 = Window.partitionBy("region").orderBy("prev_sqft", "id")
w11 = Window.partitionBy("region").orderBy("next_sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-09-26").cast("date"))
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.upper("store_name").alias("store_name_upper"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 1), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.ntile(10).over(w2).alias("sqft_bucket"),
        F.lead("sqft", 1).over(w3).alias("next_sqft"),
        F.lag("sqft", 2, 0).over(w3).alias("prev_sqft"),
    )
)

metrics_stores = (
    metrics
    .withColumn("cumulative_sqft_bucket", F.sum("sqft_bucket").over(w4))
    .withColumn("next_sqft_bucket", F.lead("sqft_bucket", 2).over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w7))
)

metrics_stores_2 = (
    metrics_stores
    .withColumn("region_sum_cumulative_sqft", F.sum("cumulative_sqft").over(w8))
    .withColumn("next_sqft_avg_6_prior", F.avg("next_sqft").over(w_running))
    .withColumn("next_sqft_share", F.round((F.col("next_sqft") / F.sum("next_sqft").over(w8)), 4))
)

with_windows = (
    metrics_stores_2
    .select(
        "*",
        F.lag("seq_no", 1).over(w10).alias("prev_seq_no"),
        F.sum("next_sqft").over(w11).alias("cumulative_next_sqft"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.col("next_sqft_bucket").cast("double").alias("next_sqft_bucket_str"),
        F.round((F.col("prev_seq_no") / F.col("cumulative_sqft")), 2).alias("prev_seq_no_per_cumulative_sqft"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "region",
        "next_sqft_bucket_str",
        "sqft_percent_rank",
        "next_sqft_share",
    )
    .unpivot(
        ["id", "region"],
        ["next_sqft_bucket_str", "sqft_percent_rank", "next_sqft_share"],
        "kpi",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
