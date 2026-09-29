from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region").isin(["East", "Central"]) & F.col("city").isin(["New York", "Bangkok", "Chiang Mai"])) | (F.col("region") != "South"))
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.when(F.col("sqft") >= 15751, "high").when(F.col("sqft") >= 4757, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") < 5)
    .drop("unique_citys")
)
