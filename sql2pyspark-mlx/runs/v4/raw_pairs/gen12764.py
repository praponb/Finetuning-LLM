from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["UK", "US"])
    .agg(F.max("rating").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
