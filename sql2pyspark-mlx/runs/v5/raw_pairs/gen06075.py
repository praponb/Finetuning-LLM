from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

with_dims = (
    exploded
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.*", F.col("p.country"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.signup_date") < "2024-07-01")
    .select(
        "src.*",
        F.col("p.country").alias("customer_country"),
        F.col("p.email"),
        F.col("p.city"),
    )
)

joined_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.code",
        "e.customer_country",
        "e.email",
        F.col("b.signup_date"),
    )
)

filtered = (
    joined_carts
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-01")))
    .select(
        "id",
        "created_at",
        "code",
        "email",
        F.trim("code").alias("clean_code"),
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("OTHER")).alias("code_clean"),
    )
)

base = (
    filtered
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-01"))) & (F.col("code").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("email_slug", F.regexp_replace("email", " ", "-"))
    .withColumn("code_filled", F.coalesce("code", F.lit("NONE")))
)

result = (
    base
    .select(
        "created_at",
        "email",
        "code_lower",
        "code_clean",
        "email_slug",
        "code_filled",
    )
)
