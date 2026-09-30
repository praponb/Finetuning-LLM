from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("age") >= 63, "high").when(F.col("age") >= 30, "mid").otherwise("low").alias("age_bucket"))
    .pivot("country", ["UK", "DE"])
    .agg(F.avg("credit_limit"))
)
