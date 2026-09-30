from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 43)
    .orderBy("avg_credit_limit", "country")
    .limit(5)
)
