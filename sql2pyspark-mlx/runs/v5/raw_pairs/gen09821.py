from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([12, 18, 37, 38]))
    .select("shipped_date")
    .union(df_products.select("launch_date"))
)
