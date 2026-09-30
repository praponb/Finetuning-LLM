from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 15) & F.col("method").isin(["cash", "bank_transfer", "promptpay"]))
)
