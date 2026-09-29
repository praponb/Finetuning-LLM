from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "UK") | (F.col("credit_limit") >= 12900))
    .groupBy("city", "country")
    .agg(F.count("*").alias("customer_count"))
)
