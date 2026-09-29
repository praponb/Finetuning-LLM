from pyspark.sql import functions as F

src = df_stores.filter(F.col("region") == "North")

summary = (
    src
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)

result = summary.select("region", "avg_sqft", "new_york_count")
