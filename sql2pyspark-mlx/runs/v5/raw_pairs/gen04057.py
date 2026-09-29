from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["SG", "TH", "UK", "JP"])
    .agg(F.sum("age"))
    .orderBy("segment")
)
