from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["US", "JP", "UK"])
    .agg(F.avg("age"))
    .orderBy("city")
)
