from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 57, "high").when(F.col("age") >= 37, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
    .orderBy("age_bucket")
)
