from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]) & F.month("end_date").isin([3, 8, 9]))
    .select("start_date", "customer_id", "status")
)
