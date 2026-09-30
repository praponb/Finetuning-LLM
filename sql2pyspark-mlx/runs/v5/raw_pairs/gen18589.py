from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2024-10-01") | ~F.col("method").isin(["card", "promptpay", "bank_transfer"]))
    .select("id", "amount", F.to_date("paid_at").alias("paid_at_date"))
)
