from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .groupBy("status", F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(F.min("shipped_date").alias("first_shipped_date"))
    .orderBy("first_shipped_date")
)
