from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("channel").eqNullSafe(F.lit("partner"))))
    .withColumn("order_date_minus_83d", F.date_sub("order_date", 83))
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.segment"), F.col("p.email"))
)

with_window = (
    joined
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest_match = (
    with_window
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("monthly_fee") >= 191, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .filter(F.col("distinct_status") > 1)
)
