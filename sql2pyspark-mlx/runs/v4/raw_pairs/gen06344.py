from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("credit_limit") >= 14600)

agg = (
    filtered_customers
    .groupBy("is_active")
    .agg(
        F.sum("age").alias("total_age"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
)

result = (
    agg
    .filter(F.col("max_credit_limit") > 10200)
    .select("is_active", "total_age", "max_credit_limit")
    .orderBy(F.col("max_credit_limit").desc())
)
