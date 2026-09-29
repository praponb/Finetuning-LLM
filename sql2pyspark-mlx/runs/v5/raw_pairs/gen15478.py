from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") >= "2024-08-01") & F.col("order_id").isNotNull())
    .select("id", "carrier", "delivered_date", "order_id")
)
