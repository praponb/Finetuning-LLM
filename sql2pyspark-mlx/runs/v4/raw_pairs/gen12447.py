from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter((F.col("segment") != "Corporate") & (F.col("is_active") | (F.col("age") == 58)))
    .withColumn("city_group", F.when(F.col("city").isin(["Berlin", "Singapore"]), "Core").otherwise("Other"))
)

customers_stats = (
    valid_customers
    .groupBy(F.year("signup_date").alias("signup_year"), "city")
    .agg(F.count("*").alias("row_count"))
)

result = (
    customers_stats
    .select("signup_year", "city", "row_count")
    .orderBy("row_count")
)
