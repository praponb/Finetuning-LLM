from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc_nulls_first(), "id")

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-11-14"))) & (F.col("plan") == "enterprise"))
    .withColumn("start_date_label", F.date_format("start_date", "MMM yyyy"))
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

attributed = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.end_date") >= F.col("sub.start_date")) & (F.col("s.end_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

attributed_subscriptions = (
    attributed
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

with_dims = (
    attributed_subscriptions
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.plan",
        "s.end_date",
        "s.monthly_fee",
        "s.status",
        "s.start_date_label",
        "s.subscription_id",
        "s.subscription_plan",
        "s.subscription_monthly_fee",
        "s.subscription_id_2",
        F.col("d.city"),
        F.col("d.signup_date"),
        F.col("d.email"),
    )
)

totals = (
    with_dims
    .groupBy(
        F.when(F.col("monthly_fee") >= 195, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_rows"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
)

result = (
    totals
    .select(
        "monthly_fee_tier",
        "num_plans",
        "active_rows",
        "monthly_fee_stddev",
        "num_cities",
    )
    .orderBy(F.col("monthly_fee_stddev").desc_nulls_last())
)
