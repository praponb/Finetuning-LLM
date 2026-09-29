from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2024-01-18")
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 254, "high").when(F.col("monthly_fee") >= 68, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.min("start_date").alias("first_start_date"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 5)
    .drop("cnt")
)
