from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

lines = (
    exploded
    .select("id", "created_at", "promo", F.explode_outer("coupon_codes").alias("code"))
)

filtered = (
    lines
    .filter(F.col("created_at") > F.lit("2025-03-01 18:00:00").cast("timestamp"))
    .withColumn("code_filled", F.coalesce("code", F.lit("N/A")))
)

cleaned = (
    filtered
    .filter(F.col("created_at") > F.lit("2025-03-01 23:00:00").cast("timestamp"))
    .select("id", "code", "code_filled", F.substring("code", 1, 2).alias("code_prefix"))
)

base = (
    cleaned
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("code_lower", F.lower("code"))
)

result = (
    base
    .select("id", "code", "code_filled", "code_prefix", "code_lower")
    .orderBy(F.col("code").desc_nulls_first(), F.col("id").desc())
)
