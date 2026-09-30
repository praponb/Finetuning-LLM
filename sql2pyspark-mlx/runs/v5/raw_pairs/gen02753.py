from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.countDistinct("credit_limit").alias("unique_countrys"),
        F.avg("age").alias("avg_age"),
    )
    .orderBy("avg_age", F.col("unique_countrys").desc())
)
