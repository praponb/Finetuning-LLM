from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("units", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id")
w_part = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w5 = Window.orderBy("units", "id")
w_ordered = Window.partitionBy("store_id").orderBy("units_bucket", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w8 = Window.orderBy("last7_count_revenue_percent_rank_whole", "id")
w9 = Window.orderBy("last7_count_revenue_percent_rank_whole", "id").rowsBetween(-5, -1)
w10 = Window.orderBy("last7_count_revenue_percent_rank_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("units_share", "id")
w12 = Window.orderBy(F.col("event_seq").desc(), "id")

scored = (
    df_store_sales
    .select(
        "*",
        F.ntile(10).over(w_recent).alias("units_bucket"),
        F.ntile(4).over(w2).alias("units_bucket_2"),
        F.round((F.col("units") / F.sum("units").over(w3)), 4).alias("units_share"),
        F.percent_rank().over(w_part).alias("revenue_percent_rank"),
        F.ntile(4).over(w5).alias("units_bucket_3"),
    )
)

scored_store_sales = (
    scored
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "units_share",
        "revenue_percent_rank",
        F.avg("revenue_percent_rank").over(w_ordered).alias("last7_avg_revenue_percent_rank"),
        F.count("revenue_percent_rank").over(w_ordered).alias("last7_count_revenue_percent_rank"),
        F.min("revenue_percent_rank").over(w_ordered).alias("last7_min_revenue_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

valid = (
    scored_store_sales
    .filter(F.col("last7_avg_revenue_percent_rank") > 0.54)
    .select(
        "id",
        "units",
        "revenue",
        "units_share",
        "last7_min_revenue_percent_rank",
        "event_seq",
        F.floor("last7_count_revenue_percent_rank").alias("last7_count_revenue_percent_rank_whole"),
    )
)

with_windows = (
    valid
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("units_avg_5_prior", F.avg("units").over(w9))
    .withColumn("cumulative_units_share", F.sum("units_share").over(w10))
)

scored_store_sales_2 = (
    with_windows
    .filter(F.col("units") > 23)
    .select(
        "*",
        F.lag("last7_min_revenue_percent_rank").over(w11).alias("prev_last7_min_revenue_percent_rank"),
        F.lead("last7_min_revenue_percent_rank").over(w11).alias("next_last7_min_revenue_percent_rank"),
        F.lag("units").over(w11).alias("prev_units"),
        F.lead("units").over(w11).alias("next_units"),
        F.row_number().over(w11).alias("seq_no_2"),
        F.ntile(10).over(w12).alias("event_seq_bucket"),
    )
)

result = (
    scored_store_sales_2
    .groupBy(
        F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
        "seq_no",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("last7_min_revenue_percent_rank", 0.9).alias("last7_min_revenue_percent_rank_p90"),
        F.percentile_approx("last7_min_revenue_percent_rank", [0.25, 0.5, 0.75]).alias("last7_min_revenue_percent_rank_quartiles"),
        F.round(F.avg("last7_min_revenue_percent_rank"), 2).alias("avg_last7_min_revenue_percent_rank"),
        F.round(F.avg("seq_no_2"), 2).alias("avg_seq_no_2"),
    )
    .filter(F.col("avg_last7_min_revenue_percent_rank") >= 0)
)
