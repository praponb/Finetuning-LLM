from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("end_date") <= "2025-03-01") & F.col("start_date").between("2023-07-01", "2025-01-08"))
    .select("id", "customer_id", "monthly_fee", F.upper("status").alias("status_upper"))
)
