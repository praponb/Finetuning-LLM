from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("payment_amount").desc(), "id", "subscription_id")

cleaned = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("status") == "SHIPPED", 1).otherwise(0).alias("is_shipped"),
    )
)

matched = (
    cleaned
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_last = (
    matched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.is_shipped",
        "s.subscription_id",
        "s.monthly_fee",
        F.col("c.status").alias("payment_status"),
        F.col("c.amount").alias("payment_amount"),
    )
)

scored = (
    with_last
    .filter(F.col("customer_id").isin([29, 32]))
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "is_shipped",
        "subscription_id",
        F.min("discount").over(w2).alias("last7_min_discount"),
        F.avg("discount").over(w2).alias("last7_avg_discount"),
        F.count("discount").over(w2).alias("last7_count_discount"),
        F.ntile(4).over(w3).alias("payment_amount_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "order_date",
        "amount",
        "is_shipped",
        "subscription_id",
        "last7_min_discount",
        "last7_avg_discount",
    )
    .orderBy(F.col("order_date").desc_nulls_first(), F.col("is_shipped").desc())
)
