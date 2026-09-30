from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") != "Tokyo")
    .groupBy(
        F.when(F.col("age") >= 59, "high").when(F.col("age") >= 26, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.max("age").alias("max_age"))
    .filter(F.col("max_age") < 40)
    .select("age_bucket", "max_age")
    .orderBy(F.col("max_age").desc())
)
