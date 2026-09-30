from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

lines = (
    exploded
    .select("id", "created_at", "value", F.explode_outer("coupon_codes").alias("code"))
)

matched_keys = (
    lines
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.hour("created_at").between(8, 11))
        .select("id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("code").isNotNull() & (F.col("created_at") >= "2023-10-08"))
    .select(
        "*",
        F.lower("code").alias("code_lower"),
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("code", F.lit("OTHER")).alias("resolved_code"),
    )
)

filtered_carts = (
    filtered
    .filter((F.col("code").eqNullSafe(F.lit("VIP"))) & (F.col("created_at_hour") != 47))
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

result = (
    filtered_carts
    .select(
        "id",
        "code",
        "code_lower",
        "created_at_hour",
        "resolved_code",
        "days_since_created_at",
        "code_clean",
        F.when(F.col("code").rlike("^(BUNDLE|SPRING10)$"), "CORE").when(F.col("code").rlike("^(VIP|WELCOME5)$"), "EXTENDED").otherwise("PRIORITY").alias("code_zone"),
    )
)
