from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.col("sqft").cast("double").alias("sqft_num"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

result = (
    valid
    .select("id")
    .exceptAll(
        df_stores
        .filter(F.col("city") == "New York")
        .select("id")
    )
)
