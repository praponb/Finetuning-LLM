from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "basic", "enterprise"]) & (F.col("plan") == "standard") & F.col("monthly_fee").between(150, 160))
)

subscriptions_stats = (
    src
    .groupBy("plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("monthly_fee")).otherwise(0)).alias("enterprise_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("enterprise_monthly_fee") >= 46)
    .select("plan", "unique_plans", "enterprise_monthly_fee")
    .orderBy("plan", F.col("enterprise_monthly_fee").desc())
)
