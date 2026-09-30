from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 7) & F.col("supplier_id").isin([4, 30]))
    .select("id", "launch_date", "stock_qty", "supplier_id")
    .orderBy("id", F.col("supplier_id").desc())
)
