from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 6, 10]) & F.col("store_id").isin([1, 2, 3, 4]) & F.col("store_id").isin([1, 4, 5, 6]))
)

store_id_summary = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    store_id_summary
    .select("store_id", "min_sale_date")
    .orderBy(F.col("store_id").desc(), F.col("min_sale_date").desc())
)
