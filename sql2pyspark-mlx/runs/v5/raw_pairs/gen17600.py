from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id").orderBy(F.col("shipping_fee").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

deduped = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.rn",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.status") == "IN_TRANSIT"), "left_anti")
)

with_dims = (
    not_flagged
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("src.*", F.col("c.name"), F.col("c.city"))
)

not_flagged_orders = (
    with_dims
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.amount") >= 820), "left_anti")
)

result = (
    not_flagged_orders
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        "shipping_fee",
        "rn",
        "name",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
