from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["London", "Tokyo", "Chiang Mai", "New York"])
    .agg(F.max("credit_limit"))
    .orderBy("segment")
)
