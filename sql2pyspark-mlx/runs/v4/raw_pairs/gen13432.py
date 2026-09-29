from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock_plus_months", "id")
w_grp_2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("event_seq_3").desc_nulls_first())
w6 = Window.partitionBy("product_id").orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("last_restock_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w12 = Window.partitionBy("product_id").orderBy("row_seq", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") < "2025-08-01")
    .select(
        "id",
        "product_id",
        "last_restock",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)

scored = (
    base
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq_2", F.row_number().over(w3))
    .withColumn("event_seq_3", F.row_number().over(w_grp_2))
)

with_windows = (
    scored
    .withColumn("event_seq_3_cume_dist", F.cume_dist().over(w5))
    .withColumn("running_event_seq_3", F.sum("event_seq_3").over(w6))
    .withColumn("event_seq_3_count_90d", F.count("event_seq_3").over(w7))
)

with_windows_inventory = (
    with_windows
    .select(
        "*",
        F.first("event_seq_3_count_90d", ignorenulls=True).over(w8).alias("first_known_event_seq_3_count_90d"),
        F.sum("event_seq").over(w9).alias("event_seq_sum_7d"),
        F.sum("running_event_seq_3").over(w_running).alias("cumulative_running_event_seq_3"),
        F.max("event_seq_3_cume_dist").over(w_recent).alias("event_seq_3_cume_dist_max_30d"),
        F.ntile(10).over(w12).alias("row_seq_bucket"),
    )
)

result = (
    with_windows_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "last_restock_period",
        "last_restock_plus_months",
        "event_seq",
        "event_seq_2",
        "event_seq_3",
        "event_seq_3_cume_dist",
    )
    .orderBy(F.col("event_seq_2").desc())
)
