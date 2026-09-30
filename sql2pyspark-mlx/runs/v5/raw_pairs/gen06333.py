from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("quantity_rounded", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded_bucket").desc())
w7 = Window.partitionBy("product_id").orderBy("quantity_avg_5_prior", "id")
w8 = Window.orderBy("quantity_avg_5_prior")

filtered = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

enriched = (
    filtered
    .filter(F.col("product_id").isin([20, 30, 36, 39]))
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_rounded",
        F.last("quantity_rounded", ignorenulls=True).over(w).alias("last_known_quantity_rounded"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.ntile(5).over(w3).alias("quantity_rounded_bucket"),
        F.ntile(4).over(w_grp).alias("quantity_rounded_bucket_2"),
        F.avg("quantity").over(w_running).alias("quantity_avg_5_prior"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "quantity",
        "last_known_quantity_rounded",
        "seq_no",
        "quantity_rounded_bucket_2",
        "quantity_avg_5_prior",
        F.sum("quantity_avg_5_prior").over(w5).alias("running_quantity_avg_5_prior"),
        F.cume_dist().over(w6).alias("quantity_rounded_bucket_cume_dist"),
        F.row_number().over(w7).alias("row_seq"),
        F.percent_rank().over(w8).alias("quantity_avg_5_prior_percent_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("quantity_avg_5_prior_percent_rank") <= 0.47)
    .select(
        "*",
        (F.col("running_quantity_avg_5_prior") - F.col("quantity_avg_5_prior_percent_rank")).alias("running_quantity_avg_5_prior_minus_quantity_avg_5_prior_percent_rank"),
    )
)

result = (
    cleaned
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("quantity_rounded_bucket_cume_dist", 0.95).alias("quantity_rounded_bucket_cume_dist_p95"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.avg("running_quantity_avg_5_prior").alias("avg_running_quantity_avg_5_prior"),
    )
)
