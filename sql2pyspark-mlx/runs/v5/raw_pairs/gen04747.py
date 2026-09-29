from pyspark.sql import functions as F

avg_credit_limit = df_customers.filter(F.col("signup_date") > "2024-02-01").agg(F.avg("credit_limit")).first()[0]

result = (
    df_customers
    .filter(F.col("credit_limit") >= avg_credit_limit)
    .select("id", "credit_limit", "city", "segment")
    .orderBy("city", F.col("id").desc())
)
