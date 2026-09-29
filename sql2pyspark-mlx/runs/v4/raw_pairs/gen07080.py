from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())
w2 = Window.orderBy("discount_rounded", "id")
w3 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

cleaned = (
    df_orders
    .filter(F.col("channel") != "partner")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.round("discount", 0).alias("discount_rounded"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.first("discount_rounded", ignorenulls=True).over(w3).alias("first_known_discount_rounded"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("payment_status"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

result = (
    with_latest
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "discount_rounded",
        "dedup_rank",
        F.when(F.col("channel").rlike("^(partner|store)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("channel_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("dedup_rank").asc_nulls_first())
)
