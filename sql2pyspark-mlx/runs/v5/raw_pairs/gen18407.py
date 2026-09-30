from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(((F.col("sale_date") > "2025-01-29") & F.col("store_id").isin([1, 3, 5, 6])) | F.col("revenue").between(3300, 3500))
)

store_id_summary = (
    src
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    store_id_summary
    .filter(F.col("num_store_sales") > 7)
    .orderBy("latest_sale_date", "num_store_sales", "store_id")
    .limit(3)
)
