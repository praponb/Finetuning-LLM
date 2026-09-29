from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("segment", ["Corporate", "Consumer"])
    .agg(F.avg("credit_limit"))
)
