from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("segment", "city").orderBy("subscription_start_date", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("signup_date") > F.to_date(F.lit("2023-02-06"))))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        F.lower("city").alias("city_lower"),
        F.when(F.col("segment").isin(["Consumer", "Corporate"]), "Core").otherwise("Other").alias("segment_group"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.segment",
        "s.city_lower",
        F.col("c.country").alias("session_country"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.plan").alias("subscription_plan"),
        F.col("c.start_date").alias("subscription_start_date"),
    )
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_latest_customers = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.segment",
        "s.city_lower",
        F.col("c.plan").alias("subscription_plan_2"),
        F.col("c.monthly_fee"),
    )
)

result = (
    with_latest_customers
    .groupBy(
        "segment",
        F.when(F.col("monthly_fee") >= 249, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.countDistinct("subscription_plan_2").alias("distinct_subscription_plan_2s"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("monthly_fee")).otherwise(0)).alias("smb_monthly_fee"),
        F.avg(F.when(F.col("subscription_plan_2") == "basic", F.col("monthly_fee"))).alias("basic_avg_monthly_fee"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
    )
    .filter(F.col("smb_monthly_fee") < 1000000)
    .orderBy(F.col("basic_avg_monthly_fee").desc())
)
