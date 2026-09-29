from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Rive%"))
    .select(
        "id",
        "sqft",
        F.initcap(F.trim("city")).alias("city_title"),
        F.concat_ws("-", "region", "store_name").alias("region_store_name_key"),
    )
    .orderBy("city_title")
)
