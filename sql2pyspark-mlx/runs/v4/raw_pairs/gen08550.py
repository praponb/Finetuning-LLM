from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 5, 10]))
    .select("store_id", F.add_months("sale_date", 6).alias("sale_date_plus_months"))
)
