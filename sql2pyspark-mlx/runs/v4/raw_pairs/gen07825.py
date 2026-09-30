from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date")
    .union(df_products.filter(F.col("cost") >= 630).select("launch_date"))
    .distinct()
)
