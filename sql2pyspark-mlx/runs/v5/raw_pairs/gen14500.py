from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") >= 85)
    .select("order_id", "status", "carrier")
)
