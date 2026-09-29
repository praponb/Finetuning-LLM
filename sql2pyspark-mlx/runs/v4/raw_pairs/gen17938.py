from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .pivot("country", ["JP", "DE"])
    .agg(F.avg("credit_limit"))
)
