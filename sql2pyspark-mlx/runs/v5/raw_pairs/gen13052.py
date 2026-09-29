from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 1460)
    .select("id", "method", "status", "amount")
)
