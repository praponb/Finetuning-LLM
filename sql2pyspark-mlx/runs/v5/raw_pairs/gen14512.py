from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        F.col("c.method"),
        F.col("c.status").alias("payment_status"),
    )
)

result = (
    with_latest
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "discount",
        "method",
        "payment_status",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "GROUP_A").when(F.col("channel").rlike("^(web|partner)$"), "GROUP_B").otherwise("EXTENDED").alias("channel_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("amount").asc_nulls_first())
)
