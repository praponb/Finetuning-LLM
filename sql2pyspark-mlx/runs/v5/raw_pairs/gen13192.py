from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w4 = Window.orderBy("last_restock_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last3_avg_quantity", "id")
w6 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("seq_no_2", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-05-28").cast("date")) & F.col("store_id").isin([1, 3, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
        F.add_months("last_restock", 3).alias("last_restock_plus_months"),
    )
)

enriched = (
    filtered
    .filter(F.col("store_id").isin([1, 2, 6]))
    .select(
        "id",
        "store_id",
        "last_restock_plus_months",
        F.min("quantity").over(w).alias("last3_min_quantity"),
        F.count("quantity").over(w).alias("last3_count_quantity"),
        F.avg("quantity").over(w).alias("last3_avg_quantity"),
        F.sum("quantity").over(w).alias("last3_sum_quantity"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "last_restock_plus_months", "last3_avg_quantity", "seq_no")
)

with_windows = (
    ranked
    .select(
        "id",
        "last_restock_plus_months",
        F.first("last3_avg_quantity", ignorenulls=True).over(w4).alias("first_known_last3_avg_quantity"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.last("last3_avg_quantity", ignorenulls=True).over(w6).alias("last_known_last3_avg_quantity"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("first_known_last3_avg_quantity").over(w7).alias("prev_first_known_last3_avg_quantity"),
        F.lag("last_known_last3_avg_quantity").over(w7).alias("prev_last_known_last3_avg_quantity"),
        F.lead("last_known_last3_avg_quantity").over(w7).alias("next_last_known_last3_avg_quantity"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "last_restock_plus_months",
        "first_known_last3_avg_quantity",
        "seq_no_2",
        "last_known_last3_avg_quantity",
        "prev_first_known_last3_avg_quantity",
        "prev_last_known_last3_avg_quantity",
        "next_last_known_last3_avg_quantity",
        "event_seq",
    )
    .orderBy(
        F.col("first_known_last3_avg_quantity").asc_nulls_last(),
        F.col("seq_no_2").desc(),
    )
)
