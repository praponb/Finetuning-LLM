from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "launch_date", "stock_qty", "brand")
    .orderBy(F.col("id").desc())
    .limit(10)
)
