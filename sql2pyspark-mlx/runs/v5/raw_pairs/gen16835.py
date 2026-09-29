from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("is_active") & ~F.col("is_active"))
    .withColumn("credit_limit_per_age", F.round((F.col("credit_limit") / F.col("age")), 2))
)

agg = (
    recent_customers
    .groupBy("segment")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("credit_limit_per_age")).otherwise(0)).alias("tokyo_credit_limit_per_age"),
    )
)

result = agg
