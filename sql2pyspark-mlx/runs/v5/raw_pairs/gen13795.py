from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.revenue").between(1800, 2700) & (F.col("ss.revenue") >= 3900))
    .select(F.col("p.product_name"), F.col("p.brand"), F.col("ss.sale_date"))
)
