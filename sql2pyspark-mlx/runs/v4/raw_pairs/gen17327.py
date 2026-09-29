from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "is_active",
        F.when(F.col("age") >= 57, "high").when(F.col("age") >= 27, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.countDistinct("age").alias("unique_countrys"))
)
