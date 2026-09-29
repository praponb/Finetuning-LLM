from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["enterprise", "basic", "premium"]) & (F.col("monthly_fee") >= 120))
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.count("*").alias("total_count"),
    )
)
