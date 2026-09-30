from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("credit_limit")).otherwise(0)).alias("de_credit_limit"),
    )
    .filter(F.col("de_credit_limit") > 34)
    .select("is_active", "de_credit_limit")
    .orderBy("de_credit_limit", "is_active")
    .limit(10)
)
