from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "Central") | F.col("store_name").like("%Tow%"))
    .withColumn("region_title", F.initcap(F.trim("region")))
    .withColumn("city_region_key", F.concat_ws("-", "city", "region"))
)
