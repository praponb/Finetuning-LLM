from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-18"))) & F.col("region").isNotNull() & F.col("city").isin(["Singapore", "Berlin"]))
    .select(
        "*",
        F.concat_ws("-", "city", "region").alias("city_region_key"),
        F.col("sqft").cast("string").alias("sqft_str"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_filled"),
    )
)

result = (
    valid
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.length("store_name") < 11)
        .select("region", "id")
    )
)
