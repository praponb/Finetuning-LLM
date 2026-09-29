from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("sqft_per_berlin"),
        F.max(F.when(F.col("city") == "London", F.col("sqft"))).alias("london_max_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_per_south"),
    )
    .orderBy("sqft_per_south")
)
