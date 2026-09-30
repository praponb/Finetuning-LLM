from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Singapore"]) & ((F.col("city") == "Bangkok") | (F.col("region") != "South")))
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .orderBy(F.col("new_york_sqft").desc())
)
