from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.round("sqft", 0).alias("sqft_rounded"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 2, 6])), "left_anti")
)
