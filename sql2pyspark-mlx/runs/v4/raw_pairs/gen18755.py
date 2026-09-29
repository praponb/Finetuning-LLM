from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 26)
    .select("stock_qty", "price", "product_name")
)
