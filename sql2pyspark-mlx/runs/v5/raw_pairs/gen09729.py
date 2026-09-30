from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") > 21) & F.col("product_id").isin([2, 4, 6, 8]) & (F.col("sale_date") > "2024-06-19"))
    .select("id", "store_id", F.least("revenue", "units").alias("least_revenue_units"))
)
