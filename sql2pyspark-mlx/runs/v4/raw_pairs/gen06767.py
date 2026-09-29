from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 930)
    .select("id", "method", "paid_at")
)
