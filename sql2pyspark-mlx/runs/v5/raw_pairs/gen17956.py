from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("name").like("K%") | (F.col("segment") == "Corporate")))
    .select("is_active", "signup_date", F.col("age").cast("double").alias("age_str"))
)
