from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "old town")
    .select(
        "sqft",
        "city",
        F.lower("city").alias("city_lower"),
        F.lower("store_name").alias("store_name_lower"),
    )
)
