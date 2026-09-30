from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([1, 10]))

agg = (
    base
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = agg.select("store_id", "revenue_count")
