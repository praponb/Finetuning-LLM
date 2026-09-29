from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2023-09-01")
    .select("id", "shipped_date", "order_id", "cost")
)
