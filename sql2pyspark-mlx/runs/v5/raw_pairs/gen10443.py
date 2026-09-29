from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([11, 17, 35]) & (F.col("price") >= 1000))
    .select("cost", "tags", "supplier_id", F.lower("brand").alias("brand_lower"))
    .orderBy(F.col("brand_lower").desc(), F.col("supplier_id").desc())
)
