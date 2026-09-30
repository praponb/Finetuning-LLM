from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["JP", "UK", "US"])
    .agg(F.sum("age"))
)
