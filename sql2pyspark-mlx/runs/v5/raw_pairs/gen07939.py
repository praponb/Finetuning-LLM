from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-10-01") | (F.to_date("paid_at") < "2025-09-01"))
    .select("id", "paid_at")
)
