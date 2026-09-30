from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 8]))
    .select("id", "revenue", F.date_add("sale_date", 59).alias("sale_date_plus_59d"))
    .orderBy(F.col("revenue").desc(), F.col("id").desc())
)
