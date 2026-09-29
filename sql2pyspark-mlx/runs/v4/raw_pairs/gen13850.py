from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([5, 8, 32]) & (F.col("amount") > 1540))
    .select("id", "status")
)
