from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("status") == "FAILED") & F.col("amount").between(430, 560))
    .select("id", "status")
    .orderBy(F.col("status").desc(), "id")
)
