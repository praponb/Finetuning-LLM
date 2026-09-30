from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 670)
    .select("supplier_id", "stock_qty")
    .orderBy(F.col("stock_qty").desc(), F.col("supplier_id").desc())
)
