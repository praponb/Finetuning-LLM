from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["West", "North", "South"])
    .agg(F.max("sqft"))
    .select(
        "city",
        F.col("West").alias("west"),
        F.col("North").alias("north"),
        F.col("South").alias("south"),
    )
)
