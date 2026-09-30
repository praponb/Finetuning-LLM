from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("products.category") == "Grocery")
    .groupBy(F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"))
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 1)
    .orderBy("launch_month", F.col("store_sale_count").desc())
)
