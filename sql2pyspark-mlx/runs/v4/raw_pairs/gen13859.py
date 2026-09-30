from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(14, 34))
    .select("product_id", "units")
    .orderBy(F.col("units").desc(), F.col("product_id").desc())
)
