from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co") & (F.col("segment").isNull() | F.col("email").like("%@outlook.com")))
    .select("credit_limit", "segment", F.trim("city").alias("clean_city"))
)
