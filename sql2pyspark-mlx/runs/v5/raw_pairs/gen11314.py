from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .subtract(df_store_sales.select("sale_date"))
    .orderBy(F.col("launch_date").desc())
)
