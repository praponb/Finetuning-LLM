from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "v",
        F.explode("coupon_codes").alias("value"),
    )
)

remaining_keys = (
    lines
    .select("customer_id", "id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("customer_id", "id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("created_at") > F.lit("2025-03-02").cast("date"))
    .select(
        "customer_id",
        "created_at",
        "attr_key",
        "v",
        F.upper("attr_key").alias("attr_key_upper"),
        F.concat("attr_key", F.lit(" "), "value").alias("attr_key_value"),
        F.coalesce("value", F.lit("OTHER")).alias("value_clean"),
    )
)

cleaned = (
    filtered
    .filter((F.col("created_at") >= F.lit("2025-03-01 10:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "attr_key",
        "v",
        "attr_key_upper",
        "value_clean",
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.country"), F.col("b.segment"))
)

result = (
    with_dims
    .select("attr_key", "value_clean", "created_at_day", "segment")
    .orderBy(F.col("attr_key").desc_nulls_last(), F.col("segment").desc())
)
