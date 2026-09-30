from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 550)
    .select("id", "amount", F.upper("status").alias("status_upper"))
    .orderBy("status_upper")
)
