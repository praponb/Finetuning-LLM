from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("monthly_fee", "id", "subscription_id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isin([36, 38, 40]))
    .withColumn("monthly_fee_band", F.when(F.col("monthly_fee") >= 242, "High").when(F.col("monthly_fee") >= 76, "Medium").otherwise("Low"))
    .withColumn("monthly_fee_scaled", F.round((F.col("monthly_fee") * 1.1), 2))
)

matched = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.end_date") >= F.col("sub.start_date")) & (F.col("s.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

joined = (
    matched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.credit_limit") < 15000)
    .select("a.*", F.col("c.name"), F.col("c.segment"))
)

metrics = (
    joined
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee_scaled",
        "subscription_id",
        "subscription_monthly_fee",
        "name",
        F.lead("monthly_fee_scaled").over(w_recent).alias("next_monthly_fee_scaled"),
        F.lag("monthly_fee_scaled").over(w_recent).alias("prev_monthly_fee_scaled"),
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_recent).alias("prev_monthly_fee"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

result = (
    metrics
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee_scaled",
        "subscription_id",
        "subscription_monthly_fee",
        "name",
        "next_monthly_fee_scaled",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("plan").asc_nulls_last(), F.col("subscription_id").desc())
)
