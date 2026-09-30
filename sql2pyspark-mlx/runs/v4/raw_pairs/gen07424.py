from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") > "2024-08-01") & F.col("end_date").between("2025-02-01", "2025-04-01"))
    .groupBy("plan")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") < 160)
)
