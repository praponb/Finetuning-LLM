from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") <= "2025-11-01") & F.col("store_id").isin([2, 4]))
    .select("id", "units", F.add_months("sale_date", 3).alias("sale_date_plus_months"))
)
