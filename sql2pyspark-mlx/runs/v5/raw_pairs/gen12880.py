from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Bangkok", "Singapore"])
    .agg(F.max("sqft").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
