from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") != "Singapore") & F.col("region").isin(["North", "West", "South"]) & F.col("city").isin(["Chiang Mai", "London"]))
    .select("id", "sqft", "opened_date", "region", F.lower("city").alias("city_lower"))
    .orderBy(F.col("sqft").desc())
)
