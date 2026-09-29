from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(5740, 12416))
    .select(
        "id",
        "store_name",
        "sqft",
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
        F.lower("city").alias("city_lower"),
    )
    .orderBy("city_lower", "id")
    .limit(10)
)
