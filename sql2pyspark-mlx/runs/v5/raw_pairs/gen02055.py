from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w4 = Window.orderBy("last_restock", "id")
w5 = Window.orderBy(F.col("quantity").desc(), "id")
w_ordered = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("quantity_percent_rank").desc(), "id")

with_windows = (
    df_inventory
    .filter(F.col("last_restock") > "2024-09-13")
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_6"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
        F.lead("quantity", 2).over(w3).alias("next_quantity"),
        F.lead("quantity", 2).over(w4).alias("next_quantity_2"),
        F.ntile(4).over(w5).alias("quantity_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "last_restock",
        "quantity_avg_6",
        "quantity_percent_rank",
        "next_quantity",
        "quantity_bucket",
        F.sum("quantity_percent_rank").over(w_ordered).alias("running_sum_quantity_percent_rank"),
        F.count("quantity_percent_rank").over(w_ordered).alias("running_count_quantity_percent_rank"),
        F.min("quantity_percent_rank").over(w_ordered).alias("running_min_quantity_percent_rank"),
        F.max("quantity_percent_rank").over(w_ordered).alias("running_max_quantity_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

cleaned = (
    scored
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-06-24")))
    .select(
        "id",
        "next_quantity",
        "running_sum_quantity_percent_rank",
        "running_min_quantity_percent_rank",
        "event_seq",
        (F.col("quantity_bucket") * F.col("running_count_quantity_percent_rank")).alias("quantity_bucket_times_running_count_quantity_percent_rank"),
    )
)

valid = (
    cleaned
    .filter(F.col("event_seq").between(4, 6))
    .withColumn("greatest_next_quantity_running_sum_quantity_percent_rank", F.greatest("next_quantity", "running_sum_quantity_percent_rank"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("running_sum_quantity_percent_rank") >= 4.5, "PREMIUM").otherwise("STANDARD").alias("running_sum_quantity_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("running_sum_quantity_percent_rank", 0.95).alias("running_sum_quantity_percent_rank_p95"),
        F.percentile_approx("greatest_next_quantity_running_sum_quantity_percent_rank", [0.25, 0.5, 0.75]).alias("greatest_next_quantity_running_sum_quantity_percent_rank_quartiles"),
        F.max("event_seq").alias("max_event_seq"),
    )
    .filter(F.col("max_event_seq") > 0)
    .orderBy(F.col("running_sum_quantity_percent_rank_p95").asc_nulls_last())
)
