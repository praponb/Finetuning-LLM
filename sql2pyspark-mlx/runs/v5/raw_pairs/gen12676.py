from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") < 237) & (F.size("tags") >= 2) & (F.col("cost") < 430))
    .select("supplier_id")
)
