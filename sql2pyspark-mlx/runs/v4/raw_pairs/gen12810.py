from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") >= 10)
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .filter(F.col("avg_credit_limit") > 5700)
    .drop("avg_credit_limit")
    .orderBy("tokyo_count", "city")
    .limit(20)
)
