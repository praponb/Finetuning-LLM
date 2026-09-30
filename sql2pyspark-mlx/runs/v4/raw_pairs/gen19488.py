from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-08-15").cast("date"))
    .withColumn("region_title", F.initcap(F.trim("region")))
    .withColumn("resolved_region", F.coalesce("region", F.lit("N/A")))
)

result = (
    base
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "Tokyo"])
    .agg(F.max("sqft").alias("total"))
    .select(
        "region",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("Tokyo").alias("tokyo"),
    )
)
