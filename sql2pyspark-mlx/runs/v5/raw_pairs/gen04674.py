from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2025-02-01")
    .select("cost", F.month("shipped_date").alias("shipped_date_month"))
    .orderBy("shipped_date_month")
)
