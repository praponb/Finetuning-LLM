from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("unit_price").desc(), "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.dense_rank().over(w2).alias("unit_price_dense_rank"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.ntile(3).over(w4).alias("unit_price_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("unit_price_bucket") != 7)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "dedup_rank",
        "last_known_quantity",
        "unit_price_bucket",
        F.round("quantity", 0).alias("quantity_rounded"),
        (F.col("dedup_rank") * F.col("last_known_quantity")).alias("dedup_rank_times_last_known_quantity"),
    )
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "product_id",
        "dedup_rank",
        "last_known_quantity",
        "quantity_rounded",
        "dedup_rank_times_last_known_quantity",
    )
)
