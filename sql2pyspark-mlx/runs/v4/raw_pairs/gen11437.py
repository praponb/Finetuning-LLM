from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-10-27", "2024-12-30") & (F.col("status") != "PAUSED"))
    .select("id", "customer_id")
)
