from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Berlin", "Chiang Mai"]) & F.col("country").isin(["TH", "JP", "SG"]) & (F.col("segment") == "Corporate"))
    .groupBy("country")
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") >= 12400)
    .orderBy("country", F.col("avg_credit_limit").desc())
)
