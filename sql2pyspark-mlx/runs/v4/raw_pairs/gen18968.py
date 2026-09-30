from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("city").isin(["Chiang Mai", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
        F.when(F.col("sqft") >= 12975, "high").when(F.col("sqft") >= 4811, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.count("*").alias("num_stores"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") >= 57)
    .orderBy("city_group", "sqft_bucket")
    .limit(5)
)
