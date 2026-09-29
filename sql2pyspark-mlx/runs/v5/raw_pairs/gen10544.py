from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io") & F.col("email").like("%@company.co") & F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") > 43)
    .orderBy(F.col("avg_credit_limit").desc())
)
