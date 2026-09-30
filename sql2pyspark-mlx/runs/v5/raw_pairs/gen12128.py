from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 33) & F.col("method").isin(["bank_transfer", "cash", "card"]))
    .select("id", "amount", "method", "status")
)
