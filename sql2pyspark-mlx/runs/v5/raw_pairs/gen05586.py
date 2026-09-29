from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "channel").orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("greatest_quantity_unit_price", "id")
w4 = Window.partitionBy("channel", "order_id")
w_recent = Window.partitionBy("order_id").orderBy("prev_quantity_2", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    deduped
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"))
)

enriched = (
    joined
    .select(
        "id",
        "order_id",
        "unit_price",
        F.lag("quantity", 1, 0).over(w2).alias("prev_quantity"),
        F.lag("quantity", 1).over(w3).alias("prev_quantity_2"),
        F.corr("unit_price", "quantity").over(w4).alias("corr_unit_price_quantity"),
    )
)

enriched_order_items = (
    enriched
    .select(
        "id",
        "order_id",
        "unit_price",
        F.max("prev_quantity").over(w_recent).alias("last3_max_prev_quantity"),
        F.count("prev_quantity").over(w_recent).alias("last3_count_prev_quantity"),
        F.min("prev_quantity").over(w_recent).alias("last3_min_prev_quantity"),
    )
)

valid = (
    enriched_order_items
    .filter(F.col("last3_count_prev_quantity") <= 14)
    .select(
        "*",
        F.col("last3_max_prev_quantity").cast("int").alias("last3_max_prev_quantity_int"),
        (F.col("last3_count_prev_quantity") - F.col("last3_max_prev_quantity")).alias("last3_count_prev_quantity_minus_last3_max_prev_quantity"),
    )
)

result = (
    valid
    .select(
        "id",
        "unit_price",
        "last3_max_prev_quantity",
        "last3_count_prev_quantity",
        "last3_min_prev_quantity",
        "last3_count_prev_quantity_minus_last3_max_prev_quantity",
    )
    .orderBy(F.col("last3_min_prev_quantity").desc())
)
