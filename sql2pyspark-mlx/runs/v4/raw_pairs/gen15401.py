from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .pivot("city", ["New York", "Bangkok", "London"])
    .agg(F.avg("credit_limit"))
    .orderBy("is_active")
)
