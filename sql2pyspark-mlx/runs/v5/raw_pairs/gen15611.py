from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["DE", "JP", "UK", "US"])
    .agg(F.sum("credit_limit"))
    .orderBy("segment")
)
