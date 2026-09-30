from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["DE", "UK", "JP", "US"])
    .agg(F.avg("age"))
    .orderBy("city")
)
