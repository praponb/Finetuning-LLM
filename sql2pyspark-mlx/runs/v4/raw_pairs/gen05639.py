from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["SMB", "Consumer"])
    .agg(F.avg("credit_limit"))
)
