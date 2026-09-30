from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "city")
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .filter(F.col("total_credit_limit") > 95000)
    .select("segment", "city", "total_credit_limit")
    .orderBy("city", "total_credit_limit")
)
