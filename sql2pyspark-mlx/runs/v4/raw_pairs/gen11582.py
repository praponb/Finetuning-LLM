from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

filtered = (
    lines
    .filter(F.col("attr_key") == "utm_source")
    .select("*", F.initcap(F.trim("attr_key")).alias("attr_key_title"))
)

base = (
    filtered
    .filter(F.col("attr_key").isNotNull() & F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "*",
        F.array_contains("coupon_codes", "SPRING10").alias("is_spring10"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("attr_key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
)
