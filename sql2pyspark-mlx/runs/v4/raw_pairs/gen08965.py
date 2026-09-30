from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([3, 13]))
    .select("id", "amount")
    .orderBy(F.col("id").desc())
)
