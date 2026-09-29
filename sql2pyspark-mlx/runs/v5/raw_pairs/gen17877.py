from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("age") >= 61, "high").when(F.col("age") >= 37, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count("age").alias("age_count"))
)
