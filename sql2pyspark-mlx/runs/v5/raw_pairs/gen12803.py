from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([23, 31, 36, 38]) & (F.col("order_id").isNotNull() | (F.col("delivered_date") < "2024-01-01")))
    .select("id", "carrier", "cost")
)
