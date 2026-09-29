from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-11-04").cast("date")) & F.col("region").isNotNull())
    .select("*", F.lower("city").alias("city_lower"))
)

base_stores = (
    base
    .filter((F.col("opened_date") >= F.lit("2024-01-24").cast("date")) & F.col("region").isNotNull() & (F.upper(F.trim("store_name")) == "SIAM FLAGSHIP"))
    .select("id", "store_name", "region", F.initcap(F.trim("city")).alias("city_title"))
)

kept = (
    base_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 4), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.revenue").isNotNull()), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.quantity") <= 205), "left_semi")
)

cleaned = (
    eligible
    .filter(F.lower(F.trim("store_name")) == "riverside")
    .withColumn("store_name_length", F.length("store_name"))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

matched_keys = (
    cleaned
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("opened_date") <= "2024-04-21")
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "city_title", "region_clean")
