from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method").isin(["promptpay", "cash"]) & (F.col("amount") <= 1100)) | (F.col("amount") > 1490))
    .select("id", "method")
)
