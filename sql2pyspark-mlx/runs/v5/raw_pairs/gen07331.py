from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
)
