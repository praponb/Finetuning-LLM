from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "US")
    .groupBy("segment")
    .pivot("city", ["Bangkok", "Berlin"])
    .agg(F.avg("credit_limit"))
)
