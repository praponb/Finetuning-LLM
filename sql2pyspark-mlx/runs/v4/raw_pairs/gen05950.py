from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .withColumn("shipped_date_period", F.date_trunc("year", "shipped_date"))
)
