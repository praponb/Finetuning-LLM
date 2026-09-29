from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 8600)
    .groupBy("segment")
    .pivot("city", ["New York", "Bangkok", "London", "Tokyo"])
    .agg(F.sum("credit_limit"))
    .orderBy("segment")
)
