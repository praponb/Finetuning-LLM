from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", "stock_qty", "supplier_id")
    .orderBy(F.col("stock_qty").desc(), F.col("supplier_id").desc())
)
