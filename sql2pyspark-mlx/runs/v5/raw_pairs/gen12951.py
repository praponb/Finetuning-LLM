from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

exploded = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "value",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

base = (
    exploded
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-01"))) & (~F.col("code").eqNullSafe(F.lit("WELCOME5"))))
    .select(
        "*",
        F.upper("code").alias("code_upper"),
        F.col("created_at").cast("date").alias("created_at_day"),
    )
)

cleaned = (
    base
    .filter(F.col("created_at_day") > "2024-07-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "value",
        "code",
        "code_upper",
        F.col("created_at").cast("date").alias("created_at_day_2"),
        F.coalesce("code", F.lit("N/A")).alias("code_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .select(
        "id",
        "customer_id",
        "created_at",
        "value",
        "code_upper",
        "code_clean",
        F.trim("code").alias("clean_code"),
        F.last_day("created_at_day_2").alias("created_at_day_2_month_end"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy("customer_id")
)
