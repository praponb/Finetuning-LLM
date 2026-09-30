from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 7677) | F.col("city").isin(["Chiang Mai", "Berlin"]))
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
)
