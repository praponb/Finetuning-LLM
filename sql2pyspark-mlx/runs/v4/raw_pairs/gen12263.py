from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2025-08-01") & (F.col("status") == "PAUSED") & F.col("status").isin(["ACTIVE", "EXPIRED"]))
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"), "status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)
