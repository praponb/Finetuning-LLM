from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Berlin", "Chiang Mai", "London"])
    .agg(F.sum("sqft").alias("total"))
    .select(
        "region",
        F.col("Berlin").alias("berlin"),
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
