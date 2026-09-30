from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([4, 5, 8, 16]))
    .select("delivered_date")
    .union(df_store_sales.filter(F.col("revenue") <= 1200).select("sale_date"))
    .distinct()
)
