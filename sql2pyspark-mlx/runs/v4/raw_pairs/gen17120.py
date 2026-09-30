from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic"]) & F.col("monthly_fee").between(70, 90))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("plan") == "premium", 1).otherwise(0))) / F.count("*")), 1).alias("pct_premium"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
    .filter(F.col("pct_premium") >= 25)
    .orderBy("status")
)
