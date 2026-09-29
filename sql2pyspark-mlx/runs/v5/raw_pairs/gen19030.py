from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_whole", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w4 = Window.orderBy("last7_max_quantity", "id")
w5 = Window.orderBy("last7_min_quantity", "id")
w_hist = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 240, "High").when(F.col("quantity") >= 107, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

base = (
    filtered
    .filter(F.col("quantity") <= 128)
    .withColumn("quantity_whole", F.floor("quantity"))
)

metrics = (
    base
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_whole",
        F.min("quantity").over(w).alias("last7_min_quantity"),
        F.avg("quantity").over(w).alias("last7_avg_quantity"),
        F.max("quantity").over(w).alias("last7_max_quantity"),
        F.last("quantity_whole", ignorenulls=True).over(w2).alias("last_known_quantity_whole"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_whole",
        "last7_min_quantity",
        "last7_avg_quantity",
        "last7_max_quantity",
    )
)

cleaned = (
    latest
    .filter(F.col("product_id") == 9)
    .select(
        "id",
        "quantity",
        "last7_min_quantity",
        "last7_max_quantity",
        F.when(F.col("quantity_whole") >= 89, "High").when(F.col("quantity_whole") >= 30, "Medium").otherwise("Low").alias("quantity_whole_band"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("quantity") <= 183)
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("first_known_last7_min_quantity", F.first("last7_min_quantity", ignorenulls=True).over(w_hist))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_hist))
)

result = (
    with_windows
    .select(
        "id",
        "last7_max_quantity",
        "quantity_whole_band",
        "seq_no",
        "first_known_last7_min_quantity",
        "cumulative_quantity",
    )
)
