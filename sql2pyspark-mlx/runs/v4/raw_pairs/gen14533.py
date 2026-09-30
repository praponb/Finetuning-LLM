from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(10500, 13200))
    .agg(
        F.countDistinct("age").alias("unique_segments"),
        F.count("age").alias("age_count"),
    )
    .orderBy("unique_segments")
)
