from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("start_date").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("start_date_plus_months").asc_nulls_last(), "id", "subscription_id", "subscription_id_2")
w3 = Window.partitionBy("subscription_plan").orderBy(F.col("monthly_fee").asc_nulls_last())

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-05-25").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        F.add_months("start_date", 1).alias("start_date_plus_months"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "start_date_plus_months",
        F.row_number().over(w).alias("seq_no"),
    )
)

matched = (
    metrics
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date_plus_months") >= F.col("sub.start_date")) & (F.col("a.start_date_plus_months") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

attributed = (
    matched
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.start_date_plus_months").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

ranked = (
    attributed
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest = (
    ranked
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupBy(
        F.date_trunc("QUARTER", "start_date_plus_months").alias("quarter_start"),
        "subscription_plan_2",
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("subscription_plan_2") == "enterprise", 1)), F.lit(0)), 2).alias("seq_no_per_enterprise"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.sum(F.when(F.col("plan") == "standard", F.col("subscription_monthly_fee")).otherwise(0)).alias("standard_subscription_monthly_fee"),
    )
    .filter(F.col("avg_seq_no") >= 0)
    .orderBy(F.col("seq_no_per_enterprise").asc_nulls_last())
)
