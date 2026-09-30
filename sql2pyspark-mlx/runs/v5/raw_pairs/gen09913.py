from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("store_id", "category").orderBy(F.col("event_seq").desc(), "id")
w_ordered = Window.orderBy(F.col("revenue_share_2").desc(), "id")
w5 = Window.orderBy("revenue_share_2", "id").rowsBetween(-3, -1)
w6 = Window.orderBy("revenue_share_2_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("event_seq").desc_nulls_last())

joined = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.id", "e.store_id", "e.product_id", "e.revenue", F.col("p.category"))
)

with_windows = (
    joined
    .select(
        "id",
        "store_id",
        "product_id",
        "category",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share_2"),
        F.stddev_samp("revenue").over(w).alias("category_stddev_samp_revenue"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "revenue_share",
        "revenue_share_2",
        "category_stddev_samp_revenue",
        "event_seq",
    )
)

enriched = (
    ranked
    .select(
        "id",
        "event_seq",
        F.ntile(10).over(w_ordered).alias("revenue_share_2_bucket"),
        F.avg("category_stddev_samp_revenue").over(w5).alias("category_stddev_samp_revenue_avg_3_prior"),
    )
)

enriched_store_sales = (
    enriched
    .filter(F.col("revenue_share_2_bucket") != 4)
    .select(
        "*",
        F.max("event_seq").over(w6).alias("running_max_event_seq"),
        F.min("event_seq").over(w6).alias("running_min_event_seq"),
        F.sum("event_seq").over(w6).alias("running_sum_event_seq"),
        F.count("event_seq").over(w6).alias("running_count_event_seq"),
        F.cume_dist().over(w7).alias("event_seq_cume_dist"),
    )
)

grouped = (
    enriched_store_sales
    .groupBy(
        F.when(F.col("running_max_event_seq") >= 6.8, "LARGE").otherwise("SMALL").alias("running_max_event_seq_tier"),
    )
    .agg(
        F.round(F.avg("running_sum_event_seq"), 2).alias("avg_running_sum_event_seq"),
        F.round(F.avg("revenue_share_2_bucket"), 2).alias("avg_revenue_share_2_bucket"),
        F.min("running_max_event_seq").alias("min_running_max_event_seq"),
        F.percentile_approx("running_max_event_seq", 0.9).alias("running_max_event_seq_p90"),
        F.max("running_min_event_seq").alias("max_running_min_event_seq"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "running_max_event_seq_tier",
        "avg_revenue_share_2_bucket",
        "max_running_min_event_seq",
        "distinct_ids",
    )
    .orderBy(F.col("avg_revenue_share_2_bucket").desc())
)
