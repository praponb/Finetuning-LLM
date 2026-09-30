from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 6, 9]))
    .select("store_id", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
)
