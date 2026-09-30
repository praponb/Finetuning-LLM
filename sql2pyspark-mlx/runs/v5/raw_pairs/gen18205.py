from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2025-02-12") & (F.col("store_name").like("%Downto%") | F.col("region").isin(["Central", "West"])))
    .select("id", "store_name", F.trim("region").alias("clean_region"))
)
