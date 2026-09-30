from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .select(
        "sqft",
        "store_name",
        "opened_date",
        F.coalesce("store_name", F.lit("Unknown")).alias("store_name_filled"),
    )
)
