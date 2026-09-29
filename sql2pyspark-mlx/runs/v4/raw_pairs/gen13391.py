from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 10699) & F.col("store_name").isNotNull())
    .groupBy("region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
)
