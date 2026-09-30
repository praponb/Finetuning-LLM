from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") > 8)
    .withColumn("credit_limit_rounded", F.round("credit_limit", 0))
    .orderBy(F.col("country").desc(), F.col("credit_limit").desc())
)
