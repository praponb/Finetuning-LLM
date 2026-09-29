from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("name") > 13) | (F.col("country") != "JP"))
    .groupBy("segment")
    .agg(
        F.min("age").alias("min_age"),
        F.max("age").alias("max_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .filter(F.col("max_age") > 47)
)
