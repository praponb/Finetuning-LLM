from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]) & F.col("revenue").between(1400, 3400))
    .select("product_id")
    .orderBy(F.col("product_id").desc())
)
