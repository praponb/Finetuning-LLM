from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2025-03-02") & (F.to_date("paid_at") >= "2023-05-17"))
    .select("id", "order_id", "paid_at", "status")
    .orderBy(F.col("order_id").desc())
)
