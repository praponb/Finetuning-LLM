from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select("order_id", "delivered_date", "shipped_date")
    .orderBy(F.col("shipped_date").desc())
)
