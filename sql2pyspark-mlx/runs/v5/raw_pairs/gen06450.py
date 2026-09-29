from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("age") < 63)

signup_quarter_summary = (
    recent_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
)

result = (
    signup_quarter_summary
    .select("signup_quarter", "chiang_mai_count")
    .orderBy(F.col("chiang_mai_count").desc())
)
