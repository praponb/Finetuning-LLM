from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        F.count("*").alias("subscription_count"),
        F.count("*").alias("num_subscriptions"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
    .filter(F.col("unique_plans") > 2)
    .drop("unique_plans")
    .orderBy(F.col("subscription_count").desc())
)
