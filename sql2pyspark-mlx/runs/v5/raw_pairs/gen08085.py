from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("status", "customer_id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

cleaned = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "shipping_fee",
        F.round("discount", 2).alias("discount_rounded"),
        (F.col("shipping_fee") + F.col("discount")).alias("shipping_fee_plus_discount"),
    )
)

matched_keys = (
    cleaned
    .select("status", "id")
    .exceptAll(
        df_orders
        .filter(F.year("order_date") == 2023)
        .select("status", "id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .withColumn("corr_shipping_fee_amount", F.corr("shipping_fee", "amount").over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
    .withColumn("corr_shipping_fee_amount_2", F.corr("shipping_fee", "amount").over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date").between("2024-02-01", "2025-04-01")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price").between(140, 390)), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (~F.col("c.method").isin(["promptpay", "card"])), "left_anti")
)

result = (
    not_flagged_orders
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "amount",
        "shipping_fee_plus_discount",
        "corr_shipping_fee_amount",
        "amount_percent_rank",
        "corr_shipping_fee_amount_2",
        F.col("min_order_amount"),
    )
    .orderBy(F.col("id").desc(), F.col("shipping_fee_plus_discount").desc())
)
