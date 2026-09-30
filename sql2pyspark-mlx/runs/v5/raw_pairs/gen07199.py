from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("first_known_unit_price").asc_nulls_last())
w6 = Window.orderBy(F.col("last_known_quantity_whole_times_unit_price").desc())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "unit_price",
        F.ntile(10).over(w).alias("unit_price_bucket"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
        F.last("unit_price", ignorenulls=True).over(w3).alias("last_known_unit_price"),
        F.first("quantity_whole", ignorenulls=True).over(w3).alias("first_known_quantity_whole"),
        F.last("quantity_whole", ignorenulls=True).over(w4).alias("last_known_quantity_whole"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("first_known_quantity_whole") >= 79)
    .withColumn("last_known_unit_price_band", F.when(F.col("last_known_unit_price") >= 340, "High").when(F.col("last_known_unit_price") >= 170, "Medium").otherwise("Low"))
    .withColumn("last_known_quantity_whole_times_unit_price", (F.col("last_known_quantity_whole") * F.col("unit_price")))
)

base = (
    cleaned
    .filter(F.col("unit_price_bucket") != 6)
    .select(
        "*",
        F.floor("last_known_quantity_whole").alias("last_known_quantity_whole_whole"),
    )
)

with_windows_order_items = (
    base
    .select(
        "*",
        F.cume_dist().over(w5).alias("first_known_unit_price_cume_dist"),
        F.dense_rank().over(w6).alias("last_known_quantity_whole_times_unit_price_dense_rank"),
    )
)

result = (
    with_windows_order_items
    .select(
        "id",
        "unit_price_bucket",
        "last_known_unit_price",
        "last_known_quantity_whole",
        "last_known_unit_price_band",
        "last_known_quantity_whole_whole",
        "last_known_quantity_whole_times_unit_price_dense_rank",
    )
    .orderBy(F.col("last_known_quantity_whole").desc())
)
