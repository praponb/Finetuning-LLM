from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w3 = Window.orderBy(F.col("unit_price").desc_nulls_first())
w4 = Window.partitionBy("order_channel").orderBy(F.col("amount").asc_nulls_last())
w5 = Window.partitionBy("order_channel").orderBy(F.col("shipping_fee").desc_nulls_last())

valid = (
    df_order_items
    .filter(F.col("order_id").isin([1, 12, 36, 38]))
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
)

with_dims = (
    valid
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.amount") < 490)
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.unit_price",
        "a.line_total",
        F.col("b.channel"),
        F.col("b.status"),
    )
)

matched_keys = (
    with_dims
    .select("order_id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") < 6)
        .select("order_id", "product_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims_order_items = (
    reconciled
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        "s.channel",
        F.col("p.discount"),
        F.col("p.amount"),
        F.col("p.order_date"),
    )
)

enriched = (
    with_dims_order_items
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.channel").alias("order_channel"), F.col("b.shipping_fee"))
)

scored = (
    enriched
    .select(
        "order_id",
        "unit_price",
        "discount",
        "order_channel",
        "shipping_fee",
        F.rank().over(w).alias("quantity_rank"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
        F.dense_rank().over(w3).alias("unit_price_dense_rank"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.dense_rank().over(w5).alias("shipping_fee_dense_rank"),
    )
)

with_dims_order_items_2 = (
    scored
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.status"))
)

result = (
    with_dims_order_items_2
    .select("order_id", "quantity_cume_dist", "unit_price")
    .unpivot(
        ids=["order_id"],
        values=["quantity_cume_dist", "unit_price"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
