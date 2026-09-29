from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", F.explode("attributes").alias("key", "attr_value"))
)

joined = (
    flattened
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .filter(F.length(F.col("d.name")) >= 8)
    .select("s.id", "s.customer_id", "s.key", "s.attr_value", F.col("d.country"))
)

cleaned = (
    joined
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "customer_id",
        "key",
        "attr_value",
        "country",
        F.substring("country", 1, 3).alias("country_prefix"),
    )
)

valid = (
    cleaned
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "key",
        "country",
        F.coalesce("country", F.lit("unassigned")).alias("country_filled"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

base = (
    valid
    .filter(~F.col("country").eqNullSafe(F.lit("SG")))
    .select(
        "id",
        "key",
        "country_filled",
        "country_clean",
        F.concat("country", F.lit("/"), "key").alias("country_key"),
        F.coalesce("key", F.lit("OTHER")).alias("resolved_key"),
    )
)

totals = (
    base
    .rollup("key")
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.size(F.collect_set("key")).alias("num_keys_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("num_keys") > 1)
)

result = (
    totals
    .select("key", "num_keys", "gift_rows", "num_keys_2")
    .orderBy(F.col("num_keys_2").desc())
)
