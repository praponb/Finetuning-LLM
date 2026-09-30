from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("product_id").isin([1, 3]))

store_id_summary = (
    src
    .groupBy("store_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    store_id_summary
    .filter(F.col("avg_revenue") > 29)
    .select("store_id", "avg_revenue")
    .orderBy("store_id", "avg_revenue")
)
