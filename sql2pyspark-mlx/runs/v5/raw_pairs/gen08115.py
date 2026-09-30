from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("is_active"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

country_summary = (
    filtered_customers
    .groupBy("country")
    .agg(F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"))
)

result = (
    country_summary
    .select("country", "bangkok_count")
    .orderBy("bangkok_count")
)
