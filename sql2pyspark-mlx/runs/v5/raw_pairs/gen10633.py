from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy("quantity", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("last_restock_day").desc(), "id")
w_part = Window.orderBy(F.col("quantity_cume_dist").desc(), "id")
w7 = Window.orderBy("first_known_last_restock_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("quantity_cume_dist_bucket", "id").rowsBetween(-2, -1)
w9 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("quantity_cume_dist_bucket", "id")
w11 = Window.orderBy(F.col("seq_no_2").desc(), "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-02-10").cast("date")) & (F.col("last_restock") < "2025-09-08"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.floor("quantity").alias("quantity_whole"),
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "last_restock_day",
        F.sum("quantity").over(w).alias("cumulative_quantity"),
        F.first("last_restock_day", ignorenulls=True).over(w2).alias("first_known_last_restock_day"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

metrics_inventory = (
    metrics
    .filter(F.col("seq_no") == 4)
    .select(
        "id",
        "quantity_cume_dist",
        "seq_no",
        "event_seq",
        F.row_number().over(w_part).alias("row_seq"),
        F.first("seq_no", ignorenulls=True).over(w7).alias("first_known_seq_no"),
        F.ntile(10).over(w_part).alias("quantity_cume_dist_bucket"),
    )
)

with_windows = (
    metrics_inventory
    .withColumn("event_seq_avg_2_prior", F.avg("event_seq").over(w8))
    .withColumn("cumulative_event_seq", F.sum("event_seq").over(w9))
    .withColumn("seq_no_2", F.row_number().over(w_part))
)

metrics_inventory_2 = (
    with_windows
    .select(
        "*",
        F.lead("quantity_cume_dist").over(w_ordered).alias("next_quantity_cume_dist"),
        F.lead("quantity_cume_dist_bucket").over(w_ordered).alias("next_quantity_cume_dist_bucket"),
        F.row_number().over(w_ordered).alias("seq_no_3"),
        F.ntile(3).over(w11).alias("seq_no_2_bucket"),
    )
)

result = (
    metrics_inventory_2
    .select(
        "id",
        "quantity_cume_dist",
        "seq_no",
        "event_seq",
        "row_seq",
        "first_known_seq_no",
        "quantity_cume_dist_bucket",
        "event_seq_avg_2_prior",
        "cumulative_event_seq",
    )
    .orderBy(F.col("seq_no").asc_nulls_first())
)
