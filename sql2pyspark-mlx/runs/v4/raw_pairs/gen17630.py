from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("subscription_id", "id", "subscription_id")

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNull())
    .withColumn("monthly_fee_band", F.when(F.col("monthly_fee") >= 198, "High").when(F.col("monthly_fee") >= 85, "Medium").otherwise("Low"))
)

with_window = (
    base
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.end_date") >= F.col("sub.start_date")) & (F.col("s.end_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

latest = (
    with_window
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

daily = (
    latest
    .groupBy(
        F.when(F.col("subscription_id") >= 9, "HIGH").otherwise("LOW").alias("subscription_id_tier"),
    )
    .agg(
        F.round(F.sum("subscription_id") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("subscription_id_per_enterprise"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_rows"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("subscription_monthly_fee")).otherwise(0)).alias("active_subscription_monthly_fee"),
    )
)

result = (
    daily
    .select(
        "subscription_id_tier",
        "subscription_id_per_enterprise",
        "active_rows",
        "num_rows",
        "active_subscription_monthly_fee",
    )
    .orderBy(F.col("active_subscription_monthly_fee").desc_nulls_first())
)
