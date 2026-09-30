from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 33)
    .select("id", "shipped_date")
)
