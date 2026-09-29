from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-12-01")
    .select("order_id")
    .intersect(df_shipments.filter(F.col("order_id") == 21).select("order_id"))
)
