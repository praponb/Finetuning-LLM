from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 10096)
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
)
