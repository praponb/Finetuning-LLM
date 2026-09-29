from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 20))
    .select("id", "status", F.upper("status").alias("status_upper"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
