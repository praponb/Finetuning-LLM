from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").isNull() & (F.upper(F.trim("store_name")) == "AIRPORT KIOSK"))
    .select(
        "id",
        "store_name",
        F.round("sqft", 0).alias("sqft_rounded"),
        F.concat_ws("/", "region", "city").alias("region_city_key"),
    )
    .orderBy(F.col("region_city_key").desc())
)
