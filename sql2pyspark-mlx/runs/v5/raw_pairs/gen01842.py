from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") == 2025)
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)
