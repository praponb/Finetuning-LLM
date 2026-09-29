from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-08-01")
    .select("product_id")
    .unionByName(df_store_sales.filter(F.col("units") > 27).select("product_id"))
)
