from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNull() & ((F.col("product_id") == 2) | F.col("product_id").isin([1, 5, 6])))
    .select("sale_date", "product_id", "units")
)
