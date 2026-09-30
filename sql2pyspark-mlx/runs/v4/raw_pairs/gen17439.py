from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") < 1130) & F.col("order_id").isin([20, 27, 30, 40])) | (F.to_date("paid_at") < "2024-10-01"))
    .select("id", "paid_at", "status", "method", F.trim("method").alias("clean_method"))
    .orderBy("paid_at", "id")
    .limit(3)
)
