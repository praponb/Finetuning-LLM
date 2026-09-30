from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.9) & F.col("supplier_name").isNotNull() & F.upper("supplier_name").like("%IAM%"))
    .select("id", "city", "rating", F.trim("city").alias("clean_city"))
)
