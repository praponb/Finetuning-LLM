from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["Tokyo", "Berlin", "Chiang Mai", "London"])
    .agg(F.max("credit_limit"))
    .orderBy("segment")
)
