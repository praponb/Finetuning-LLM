from pyspark.sql import functions as F

result = (
    df_customers
    .filter(((F.col("segment") == "Consumer") & F.upper("email").like("%VY@%")) | F.lower("name").like("%aro%"))
    .select("id", "is_active", F.trim("country").alias("clean_country"))
)
