from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "country")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("age")).otherwise(0)).alias("uk_age"),
    )
    .filter(F.col("uk_age") > 66)
    .select("is_active", "country", "uk_age")
)
