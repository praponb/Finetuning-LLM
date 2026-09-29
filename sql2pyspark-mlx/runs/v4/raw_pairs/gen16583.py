from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["London", "Bangkok"])
    .agg(F.avg("rating").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
)
