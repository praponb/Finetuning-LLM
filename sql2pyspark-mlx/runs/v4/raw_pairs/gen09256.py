from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("attr_key").isNotNull())
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("N/A")))
    .withColumn("attr_key_group", F.when(F.col("attr_key").isin(["weight_kg", "utm_source"]), "Core").otherwise("Other"))
)

remaining_keys = (
    cleaned
    .select("id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") < "2025-01-01")
        .select("id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(~F.col("attr_key").isin(["weight_kg", "device"]))
    .select(
        "*",
        F.coalesce("attr_key", F.lit("unassigned")).alias("attr_key_filled_2"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

result = (
    base
    .select(
        "id",
        "created_at",
        "attr_key",
        "attr_key_filled",
        "attr_key_group",
        "attr_key_filled_2",
        "resolved_attr_key",
    )
)
