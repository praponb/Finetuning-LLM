from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").asc_nulls_last(), "id")

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched = (
    deduped
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

joined = (
    matched
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.country"), F.col("d.signup_date"))
)

enriched = (
    joined
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.email"))
)

result = (
    enriched
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 214, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "end_date"))
    .groupingSets(
        [["monthly_fee_tier", "quarter_start"], ["monthly_fee_tier"], []],
        "monthly_fee_tier",
        "quarter_start",
    )
    .agg(
        F.sum("subscription_monthly_fee").alias("total_subscription_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("subscription_plan") == "standard", 1)), F.lit(0)), 2).alias("monthly_fee_per_standard"),
        F.round(F.sum("subscription_id") / F.nullif(F.count(F.when(F.col("subscription_plan") == "basic", 1)), F.lit(0)), 2).alias("subscription_id_per_basic"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
    )
    .filter(F.col("basic_events") < 1)
    .orderBy(F.col("subscription_id_per_basic").desc())
)
