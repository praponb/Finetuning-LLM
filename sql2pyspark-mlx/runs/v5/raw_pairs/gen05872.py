from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["DE", "UK", "SG"])
    .agg(F.count("credit_limit"))
    .orderBy("city")
)
