from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.quarter("start_date").alias("start_quarter"),
        F.month("start_date").alias("start_month"),
    )
    .agg(F.countDistinct("monthly_fee").alias("unique_plans"))
    .filter(F.col("unique_plans") >= 7)
    .select("start_quarter", "start_month", "unique_plans")
    .orderBy("unique_plans")
)
