from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("adjusted_quantity").desc(), "id")
w2 = Window.orderBy("adjusted_quantity", "id")
w3 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("running_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("last_restock", "id")

valid = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.round((F.col("quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
)

scored = (
    valid
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "adjusted_quantity",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(5).over(w2).alias("adjusted_quantity_bucket"),
        F.sum("quantity").over(w3).alias("running_quantity"),
    )
)

scored_inventory = (
    scored
    .withColumn("quantity_bucket", F.ntile(10).over(w4))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w5))
)

with_windows = (
    scored_inventory
    .withColumn("quantity_bucket_avg_3", F.avg("quantity_bucket").over(w6))
    .withColumn("last_known_adjusted_quantity", F.last("adjusted_quantity", ignorenulls=True).over(w7))
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w8))
)

joined = (
    with_windows
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .filter(F.col("d.region") != "Central")
    .select(
        "e.id",
        "e.store_id",
        "e.seq_no",
        "e.adjusted_quantity_bucket",
        "e.running_quantity",
        F.col("d.region"),
    )
)

result = (
    joined
    .rollup(
        F.when(F.col("adjusted_quantity_bucket") >= 6, "PREMIUM").otherwise("STANDARD").alias("adjusted_quantity_bucket_tier"),
        "store_id",
    )
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.round(F.avg("adjusted_quantity_bucket"), 2).alias("avg_adjusted_quantity_bucket"),
    )
)
