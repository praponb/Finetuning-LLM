from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

with_last = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("payment_status"), F.col("c.paid_at"))
)

result = (
    with_last
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "shipping_fee",
        "paid_at",
        F.when(F.col("channel").rlike("^(store|web)$"), "EXTENDED").when(F.col("channel").rlike("^(mobile|partner)$"), "CORE").otherwise("PRIORITY").alias("channel_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("amount").desc_nulls_last())
)
