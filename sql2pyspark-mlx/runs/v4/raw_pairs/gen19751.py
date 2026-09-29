from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "bank_transfer", "card"]) & F.col("order_id").isNull())
)
