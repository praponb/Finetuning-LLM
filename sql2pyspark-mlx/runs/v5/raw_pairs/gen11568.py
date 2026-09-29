from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["SG", "DE"])
    .agg(F.sum("rating").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
