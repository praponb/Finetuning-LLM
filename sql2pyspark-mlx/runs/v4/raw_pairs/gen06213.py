from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "promptpay", "bank_transfer"]))
    .select("status", "method")
    .distinct()
)
