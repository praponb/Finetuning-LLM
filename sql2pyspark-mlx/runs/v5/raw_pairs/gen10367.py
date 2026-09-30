from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2023-05-22")
    .select("revenue")
    .intersect(df_products.select("cost"))
)
