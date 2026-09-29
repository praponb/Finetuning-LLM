from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2025-01-01")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
    .orderBy("last_shipped_date")
)
