from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-07-05", "2024-11-01") & F.col("customer_id").isNull())
    .select("id", "plan", "customer_id")
)
