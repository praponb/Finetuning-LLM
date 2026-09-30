from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_last(), "id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

filtered = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-04-01")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_window = (
    ranked
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date") >= F.col("sub.start_date")) & (F.col("src.start_date") <= F.date_add(F.col("sub.end_date"), 10)), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

base = (
    with_window
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("days_since_end_date") / F.col("subscription_id")), 2).alias("days_since_end_date_per_subscription_id"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.id", "s.customer_id", "s.subscription_id", F.col("b.credit_limit"))
)

result = (
    with_dims
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "subscription_id",
        "credit_limit",
        F.col("sum_order_discount"),
    )
)
