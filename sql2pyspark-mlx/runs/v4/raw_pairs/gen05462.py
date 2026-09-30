from pyspark.sql import functions as F

joined = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.city"))
)

matched_keys = (
    joined
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date").between("2024-04-19", "2024-09-19"))
        .select("id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.opened_date"), F.col("s.store_name"))
)

joined_store_sales = (
    with_dims
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.sqft"), F.col("b.city").alias("store_city"))
)

matched_keys_2 = (
    joined_store_sales
    .select("store_id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("store_id") == 5)
        .select("store_id", "product_id")
    )
)

verified_store_sales = (
    joined_store_sales
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified_store_sales
    .filter(F.year("opened_date") == 2025)
    .select(
        "id",
        "store_id",
        "revenue",
        "city",
        "opened_date",
        "store_name",
        "sqft",
        F.upper("store_name").alias("store_name_upper"),
        F.date_sub("sale_date", 47).alias("sale_date_minus_47d"),
        F.coalesce("store_city", F.lit("OTHER")).alias("store_city_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "revenue",
        "city",
        "opened_date",
        "store_name",
        "sqft",
        "store_name_upper",
        "sale_date_minus_47d",
    )
    .orderBy(F.col("sale_date_minus_47d").desc_nulls_last())
)
