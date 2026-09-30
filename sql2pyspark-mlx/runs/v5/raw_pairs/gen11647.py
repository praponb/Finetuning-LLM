from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & (F.col("status") == "DELIVERED"))
    .select("id", "carrier", "cost", "delivered_date")
)
