from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 70)
    .select("id", "delivered_date", "status", "shipped_date")
)
