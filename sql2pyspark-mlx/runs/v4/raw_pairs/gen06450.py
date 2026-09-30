from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")
w_part = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_recent = Window.partitionBy("product_id", "order_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("product_id_sum_unit_price").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy("amount", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id") == 14)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.amount"), F.col("o.order_date"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.ntile(5).over(w_part).alias("unit_price_bucket"),
        F.sum("unit_price").over(w_recent).alias("product_id_sum_unit_price"),
    )
)

result = (
    with_windows
    .select(
        "*",
        F.ntile(10).over(w4).alias("quantity_bucket"),
        F.ntile(4).over(w5).alias("product_id_sum_unit_price_bucket"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(4).over(w6).alias("amount_bucket"),
    )
)
