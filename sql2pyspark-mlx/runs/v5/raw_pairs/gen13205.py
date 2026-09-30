from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.when(F.col("credit_limit") >= 13900, "high").when(F.col("credit_limit") >= 6900, "mid").otherwise("low").alias("credit_limit_bucket"))
    .pivot("segment", ["SMB", "Corporate"])
    .agg(F.sum("age"))
)
