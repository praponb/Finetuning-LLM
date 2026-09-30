from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.length("store_name") >= 14) & (F.col("region").isin(["North", "Central", "South"]) | (F.length("store_name") >= 12)))
    .select(
        "city",
        "sqft",
        "region",
        F.substring("store_name", 1, 4).alias("store_name_prefix"),
        F.trim("city").alias("clean_city"),
    )
)
