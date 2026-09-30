from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme", "Globex"]) & F.col("supplier_id").isin([15, 16, 31, 33]))
    .select("id", "launch_date", F.lower("product_name").alias("product_name_lower"))
)
