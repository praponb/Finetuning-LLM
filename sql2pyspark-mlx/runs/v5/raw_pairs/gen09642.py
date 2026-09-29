from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 1600) & (F.col("units") < 36))
    .select("id", "store_id", "product_id", "revenue")
    .orderBy(F.col("id").desc(), "product_id")
)
