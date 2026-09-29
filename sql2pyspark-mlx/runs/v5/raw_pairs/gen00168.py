from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

valid = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp"))
    .select("*", F.initcap(F.trim("element")).alias("element_title"))
)

cleaned = (
    valid
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.col("created_at").cast("date").alias("created_at_day_2"),
        F.coalesce("element", F.lit("N/A")).alias("element_filled"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(~F.col("b.is_active"))
    .select(
        "src.id",
        "src.customer_id",
        "src.element",
        "src.created_at_day",
        "src.element_filled",
        F.col("b.signup_date"),
        F.col("b.city"),
    )
)

matched_keys = (
    joined
    .select("customer_id", "id")
    .intersectAll(
        df_carts
        .filter(F.size("coupon_codes") >= 2)
        .select("customer_id", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("s.*", F.col("p.segment"), F.col("p.city").alias("customer_city"))
)

result = (
    enriched
    .select(
        "customer_id",
        "element",
        "element_filled",
        "city",
        "segment",
        "customer_city",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "CORE").otherwise("PRIORITY").alias("segment_zone"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
