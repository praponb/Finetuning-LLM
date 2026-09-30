from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
)
