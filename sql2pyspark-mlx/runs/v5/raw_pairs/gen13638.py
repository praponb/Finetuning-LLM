from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .groupBy("city")
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("avg_age") < 67)
    .select("city", "avg_age", "min_credit_limit")
    .orderBy(F.col("avg_age").desc())
)
