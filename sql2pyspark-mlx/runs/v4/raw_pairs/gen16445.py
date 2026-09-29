from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("revenue").isNull())

summary = (
    base
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    summary
    .orderBy(F.col("product_id").desc())
    .limit(1)
)
