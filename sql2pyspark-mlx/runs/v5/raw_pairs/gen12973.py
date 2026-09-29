from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w_ordered = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("row_seq", "id").rowsBetween(-2, -1)
w_hist = Window.orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last4_avg_row_seq", "id")
w6 = Window.orderBy("last4_max_row_seq", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") <= "2025-06-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
    )
)

metrics = base.select("*", F.row_number().over(w).alias("row_seq"))

metrics_inventory = (
    metrics
    .select(
        "id",
        "last_restock",
        "last_restock_period",
        F.sum("row_seq").over(w_ordered).alias("last4_sum_row_seq"),
        F.max("row_seq").over(w_ordered).alias("last4_max_row_seq"),
        F.avg("row_seq").over(w_ordered).alias("last4_avg_row_seq"),
        F.avg("row_seq").over(w3).alias("row_seq_avg_2_prior"),
    )
)

cleaned = (
    metrics_inventory
    .filter(F.hour("last_restock_period").between(6, 11))
    .select(
        "id",
        "last_restock_period",
        "last4_sum_row_seq",
        "last4_max_row_seq",
        "last4_avg_row_seq",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

scored = (
    cleaned
    .withColumn("running_count_last4_sum_row_seq", F.count("last4_sum_row_seq").over(w_hist))
    .withColumn("running_min_last4_sum_row_seq", F.min("last4_sum_row_seq").over(w_hist))
    .withColumn("running_sum_last4_sum_row_seq", F.sum("last4_sum_row_seq").over(w_hist))
    .withColumn("running_avg_last4_sum_row_seq", F.avg("last4_sum_row_seq").over(w_hist))
)

scored_inventory = (
    scored
    .select(
        "id",
        "days_since_last_restock",
        "running_count_last4_sum_row_seq",
        "running_min_last4_sum_row_seq",
        "running_sum_last4_sum_row_seq",
        F.lag("running_min_last4_sum_row_seq").over(w5).alias("prev_running_min_last4_sum_row_seq"),
        F.lead("running_min_last4_sum_row_seq").over(w5).alias("next_running_min_last4_sum_row_seq"),
        F.lag("running_sum_last4_sum_row_seq").over(w5).alias("prev_running_sum_last4_sum_row_seq"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(10).over(w6).alias("last4_max_row_seq_bucket"),
    )
)

result = (
    scored_inventory
    .select(
        "id",
        "days_since_last_restock",
        "running_count_last4_sum_row_seq",
        "running_min_last4_sum_row_seq",
        "prev_running_sum_last4_sum_row_seq",
        "last4_max_row_seq_bucket",
    )
    .orderBy("prev_running_sum_last4_sum_row_seq")
)
