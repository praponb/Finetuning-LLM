from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select("revenue", "product_id", F.round("revenue", 2).alias("revenue_rounded"))
)
