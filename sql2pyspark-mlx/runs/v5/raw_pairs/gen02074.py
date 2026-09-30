from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("country", ["SG", "TH"])
    .agg(F.sum("age"))
)
