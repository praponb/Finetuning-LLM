from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .groupBy("is_active")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .orderBy(F.col("avg_credit_limit").desc())
)
