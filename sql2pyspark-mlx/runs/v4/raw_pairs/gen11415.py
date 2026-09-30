from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") <= "2024-05-01") & F.col("carrier").isNull())
    .select("id", "delivered_date", "cost")
)
