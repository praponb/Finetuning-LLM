from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("country") != "JP") & (F.col("is_active") | F.col("name").isNull()))
)

customers_stats = (
    recent_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.min("signup_date").alias("min_signup_date"))
)

result = customers_stats.orderBy(F.col("signup_year").desc())
