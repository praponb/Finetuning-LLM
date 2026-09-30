from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("amount") < 1570))
    .select("id", "status", F.col("amount").cast("int").alias("amount_int"))
    .orderBy(F.col("amount_int").desc())
)
