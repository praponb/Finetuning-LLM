from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

flattened = (
    exploded
    .select("id", "customer_id", "attr_key", F.explode("coupon_codes").alias("code"))
)

joined = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.credit_limit"), F.col("d.email"))
)

kept_keys = (
    joined
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("id")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("code").isNotNull())
    .select(
        "id",
        "customer_id",
        "attr_key",
        "code",
        "credit_limit",
        "email",
        F.lower("attr_key").alias("attr_key_lower"),
    )
)

joined_carts = (
    cleaned
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.email").alias("customer_email"), F.col("d.segment"))
)

result = (
    joined_carts
    .groupBy("customer_id")
    .pivot("attr_key", ["device", "gift", "weight_kg"])
    .agg(F.max("credit_limit").alias("amount"), F.count(F.lit(1)).alias("n"))
)
