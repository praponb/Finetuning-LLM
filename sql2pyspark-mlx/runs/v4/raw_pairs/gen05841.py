from pyspark.sql import functions as F

base = df_customers.filter(F.col("credit_limit") >= 15500)

agg = (
    base
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.avg("credit_limit").alias("avg_credit_limit"))
)

result = (
    agg
    .filter(F.col("avg_credit_limit") < 14300)
    .orderBy(F.col("avg_credit_limit").desc())
)
