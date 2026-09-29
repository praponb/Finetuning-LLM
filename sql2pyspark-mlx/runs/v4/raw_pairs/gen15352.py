from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Berlin")
    .groupBy("city")
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("stddev_credit_limit") > 47)
    .orderBy("city")
    .limit(3)
)
