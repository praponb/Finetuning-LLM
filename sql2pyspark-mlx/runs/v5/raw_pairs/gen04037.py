from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "London"])
    .agg(F.max("sqft"))
    .select(
        "region",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
