from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["SG", "UK", "US", "DE"])
    .agg(F.avg("age"))
    .orderBy("segment")
)
