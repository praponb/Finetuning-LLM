from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.countDistinct("age").alias("unique_segments"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("unique_segments") > 5)
    .orderBy("segment")
)
