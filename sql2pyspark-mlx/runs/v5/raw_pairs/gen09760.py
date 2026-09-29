from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper("product_name").like("%ESK%") & F.col("brand").isNotNull())
    .select("stock_qty", "launch_date", "product_name")
    .orderBy("product_name")
)
