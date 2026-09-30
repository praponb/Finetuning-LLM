from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-04-01").cast("date")) & F.col("region").isNotNull())
    .withColumn("city_lower", F.lower("city"))
)

result = (
    base
    .groupBy("city")
    .pivot("region", ["South", "West"])
    .agg(F.avg("sqft").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
