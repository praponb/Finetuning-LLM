from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last(), "id")

base = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("opened_date").between("2025-03-01", "2025-04-30"))
    .select(
        "id",
        "region",
        "sqft",
        F.concat_ws(",", "region", "city").alias("region_city_key"),
        F.length("region").alias("region_length"),
    )
)

cleaned = (
    base
    .filter(F.col("sqft") == 7133)
    .select(
        "*",
        F.round("sqft", 0).alias("sqft_rounded"),
        F.round((F.col("region_length") * 0.9), 2).alias("region_length_scaled"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "sqft",
        "region_city_key",
        "region_length",
        "sqft_rounded",
        "region_length_scaled",
    )
)

remaining_keys = (
    deduped
    .select("id")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") >= "2023-03-01")
        .select("id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "sqft", "region_length", "sqft_rounded", "region_length_scaled")
    .orderBy(F.col("id").desc_nulls_last())
)
