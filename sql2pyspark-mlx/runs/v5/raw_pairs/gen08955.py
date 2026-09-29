from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("credit_limit") >= 13000, "high").when(F.col("credit_limit") >= 3900, "mid").otherwise("low").alias("credit_limit_bucket"))
    .pivot("segment", ["Corporate", "Consumer"])
    .agg(F.sum("credit_limit"))
)
