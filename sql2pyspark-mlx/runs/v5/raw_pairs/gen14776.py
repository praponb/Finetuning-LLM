from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("running_count_revenue", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("running_min_revenue").desc(), "id")
w6 = Window.orderBy("store_id_sum_revenue", "id")
w7 = Window.partitionBy("product_id").orderBy("seq_no", "id")
w_recent = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w12 = Window.partitionBy("product_id").orderBy("next_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("product_id").orderBy(F.col("seq_no").desc(), "id")

metrics = (
    df_store_sales
    .select(
        "*",
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.count("revenue").over(w).alias("running_count_revenue"),
        F.sum("revenue").over(w2).alias("store_id_sum_revenue"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "running_min_revenue",
        "running_count_revenue",
        F.last("store_id_sum_revenue", ignorenulls=True).over(w3).alias("last_known_store_id_sum_revenue"),
        F.lag("store_id_sum_revenue", 1).over(w4).alias("prev_store_id_sum_revenue"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "running_min_revenue",
        "last_known_store_id_sum_revenue",
        "event_seq",
        "seq_no",
        F.lead("seq_no", 1).over(w7).alias("next_seq_no"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

enriched = (
    with_windows_store_sales
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w9))
    .withColumn("cumulative_running_min_revenue", F.sum("running_min_revenue").over(w10))
    .withColumn("seq_no_2", F.row_number().over(w11))
    .withColumn("cumulative_running_min_revenue_2", F.sum("running_min_revenue").over(w12))
    .withColumn("seq_no_bucket", F.ntile(10).over(w_hist))
)

valid = (
    enriched
    .filter(F.col("next_seq_no") > 8)
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "running_min_revenue",
        "last_known_store_id_sum_revenue",
        "event_seq",
        "seq_no",
        "next_seq_no",
        "row_seq",
        "last_known_row_seq",
        "cumulative_running_min_revenue",
        "seq_no_2",
        "cumulative_running_min_revenue_2",
        "seq_no_bucket",
        F.round("seq_no_bucket", 0).alias("seq_no_bucket_rounded"),
    )
)

matched_keys = (
    valid
    .select("id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("units") >= 13)
        .select("id", "product_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("a")
    .join(df_products.alias("dim"), F.col("a.product_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.cumulative_running_min_revenue")).alias("max_cumulative_running_min_revenue"),
    )
)
