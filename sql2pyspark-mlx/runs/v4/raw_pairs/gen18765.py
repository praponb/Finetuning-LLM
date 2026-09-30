from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "city")
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 15500)
    .select("is_active", "city", "avg_credit_limit")
    .orderBy("city", "avg_credit_limit", "is_active")
    .limit(50)
)
