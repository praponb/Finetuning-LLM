from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        F.substring("store_name", 1, 2).alias("store_name_prefix"),
        F.trim("city").alias("clean_city"),
    )
)
