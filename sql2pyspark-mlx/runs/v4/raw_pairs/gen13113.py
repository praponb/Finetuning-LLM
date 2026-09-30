from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("quantity").desc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("unit_price").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

matched_keys = (
    valid
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("product_id", "order_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w))
    .withColumn("quantity_rank", F.rank().over(w2))
)

result = (
    scored
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "corr_quantity_unit_price",
        "quantity_rank",
    )
    .orderBy(F.col("quantity_rank").desc(), "corr_quantity_unit_price")
)
