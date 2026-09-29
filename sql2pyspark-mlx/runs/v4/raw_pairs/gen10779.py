from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("amount").between(640, 1110) & (F.col("amount") <= 550))
    .select("id", "method", "status")
    .orderBy("method", F.col("status").desc(), "id")
    .limit(3)
)
