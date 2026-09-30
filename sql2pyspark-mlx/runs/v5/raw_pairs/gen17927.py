from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(~F.col("plan").isin(["enterprise", "basic", "standard"]) & (F.col("customer_id") == 15))
)

monthly_fee_bucket_summary = (
    recent_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 203, "high").when(F.col("monthly_fee") >= 122, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "plan",
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"))
)

result = (
    monthly_fee_bucket_summary
    .select("monthly_fee_bucket", "plan_values")
    .orderBy("plan_values", F.col("monthly_fee_bucket").desc())
)
