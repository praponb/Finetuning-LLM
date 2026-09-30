from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1560) | F.col("status").isin(["FAILED", "REFUNDED"]))
    .select("id", "order_id")
    .orderBy(F.col("order_id").desc(), F.col("id").desc())
)
