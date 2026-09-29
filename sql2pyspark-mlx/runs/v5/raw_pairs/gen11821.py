from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .orderBy("avg_age", "city")
    .limit(10)
)
