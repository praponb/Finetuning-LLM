from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Berlin", "Singapore", "London", "Chiang Mai"])
    .agg(F.count("credit_limit"))
    .orderBy("country")
)
