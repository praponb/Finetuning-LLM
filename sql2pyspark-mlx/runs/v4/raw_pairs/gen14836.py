from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select("id", "units", "product_id", "sale_date")
    .orderBy(F.col("product_id").desc())
)
