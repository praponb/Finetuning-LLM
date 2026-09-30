from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(F.when(F.col("budget") >= 4473000, "high").when(F.col("budget") >= 673000, "mid").otherwise("low").alias("budget_bucket"))
    .pivot("location", ["London", "Chiang Mai", "Singapore", "Berlin"])
    .agg(F.sum("budget"))
    .orderBy("budget_bucket")
)
