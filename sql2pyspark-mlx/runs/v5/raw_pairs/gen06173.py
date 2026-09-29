from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_plus_50d", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w4 = Window.orderBy("last_restock_plus_50d_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("quantity", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-08-30")))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_add("last_restock", 50).alias("last_restock_plus_50d"),
    )
)

scored = (
    base
    .withColumn("prev_quantity", F.lag("quantity", 2).over(w))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w2))
)

latest = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "quantity", "last_restock", "last_restock_plus_50d")
)

base_inventory = (
    latest
    .filter(F.col("last_restock") > F.lit("2023-04-08").cast("date"))
    .withColumn("last_restock_plus_50d_period", F.date_trunc("month", "last_restock_plus_50d"))
)

with_windows = (
    base_inventory
    .filter(F.col("last_restock_plus_50d") <= "2025-05-01")
    .select(
        "id",
        "quantity",
        "last_restock",
        F.last("quantity", ignorenulls=True).over(w4).alias("last_known_quantity"),
        F.ntile(10).over(w5).alias("quantity_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-06-15")))
    .select(
        "id",
        "quantity",
        "last_restock",
        "last_known_quantity",
        "quantity_bucket",
        F.floor("last_known_quantity").alias("last_known_quantity_whole"),
    )
)

result = (
    filtered
    .select("id", "last_known_quantity", "quantity_bucket", "last_known_quantity_whole")
    .orderBy(F.col("last_known_quantity_whole").desc())
)
