from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("is_bank_transfer", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

filtered = (
    df_payments
    .filter(F.col("amount").between(700, 720))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.when(F.col("method") == "bank_transfer", 1).otherwise(0).alias("is_bank_transfer"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.shipping_fee"), F.col("d.discount"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_2_prior"),
        F.sum("shipping_fee").over(w2).alias("running_shipping_fee"),
    )
)

with_dims_payments = (
    metrics
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.amount").alias("order_amount"))
)

result = (
    with_dims_payments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "is_bank_transfer",
        "shipping_fee",
        "discount",
        "running_shipping_fee",
        F.when(F.col("method").rlike("^(cash|paypal)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("method_zone"),
        F.col("max_order_item_unit_price"),
    )
)
