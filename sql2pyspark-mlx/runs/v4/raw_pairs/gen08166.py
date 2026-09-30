from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.substring("region", 1, 4).alias("region_prefix"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("region") != "North")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "region_prefix",
        "resolved_region",
        F.lower("store_name").alias("store_name_lower"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.units") <= 25), "left_semi")
)

valid = (
    not_flagged
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "region_prefix",
        "store_name_lower",
        F.concat_ws("/", "store_name", "region").alias("store_name_region_key"),
        F.trim("region").alias("clean_region"),
    )
)

result = (
    valid
    .select(
        "id",
        "city",
        "region",
        "region_prefix",
        "store_name_lower",
        "store_name_region_key",
        "clean_region",
    )
    .orderBy("id", F.col("city").desc())
)
