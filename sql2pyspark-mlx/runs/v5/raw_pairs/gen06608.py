from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("opened_date") <= "2023-11-13")
        .select("id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("opened_date") > F.lit("2023-08-27").cast("date")) & (F.col("sqft") != 14896))
    .withColumn("store_name_title", F.initcap(F.trim("store_name")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date")), "left_semi")
)

valid = (
    not_flagged
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("East"))))
    .select(
        "*",
        F.round((F.col("sqft") * 100), 2).alias("sqft_adjusted"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

matched_keys = (
    valid
    .select("city", "region")
    .subtract(
        df_stores
        .filter(F.col("region") == "East")
        .select("city", "region")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "store_name_title",
        "region_filled",
        F.when(F.col("region").rlike("^(East|South)$"), "GROUP_A").otherwise("CORE").alias("region_zone"),
    )
    .orderBy(F.col("sqft").desc_nulls_first())
)
