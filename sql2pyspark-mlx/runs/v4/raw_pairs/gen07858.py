from pyspark.sql import functions as F

result = (
    df_customers
    .rollup("segment", "is_active")
    .agg(
        F.countDistinct("credit_limit").alias("unique_segments"),
        F.count("*").alias("num_customers"),
    )
    .orderBy(F.col("num_customers").desc())
)
