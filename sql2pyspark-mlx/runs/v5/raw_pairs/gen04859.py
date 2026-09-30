from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("revenue") < 3400)

store_sales_stats = (
    recent_store_sales
    .groupBy("store_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("num_store_sales") >= 9)
    .select("store_id", "num_store_sales")
    .orderBy(F.col("num_store_sales").desc(), "store_id")
    .limit(5)
)
