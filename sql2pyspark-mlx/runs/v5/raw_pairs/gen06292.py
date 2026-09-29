from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.col("signup_date") <= "2025-05-01"))
    .groupBy("segment")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("age")).otherwise(0)).alias("bangkok_age"),
    )
    .filter(F.col("bangkok_age") > 56)
    .drop("bangkok_age")
    .orderBy("segment")
    .limit(3)
)
