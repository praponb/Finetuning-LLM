from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([7, 9, 11]) & (F.col("sale_date") >= "2023-09-29"))
    .groupBy("store_id", "product_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)
