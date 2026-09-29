from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("credit_limit")).otherwise(0)).alias("tokyo_credit_limit"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
    )
    .orderBy(F.col("age_p90").desc())
)
