from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
        F.upper("region").alias("region_upper"),
    )
)
