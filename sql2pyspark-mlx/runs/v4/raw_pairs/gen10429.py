from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method").isin(["paypal", "cash", "bank_transfer"]) & ~F.col("method").isin(["promptpay", "card"])) | (F.to_date("paid_at") < "2025-10-01"))
    .select("id", "amount")
    .orderBy("amount")
)
