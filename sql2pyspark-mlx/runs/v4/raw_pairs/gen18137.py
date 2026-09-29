from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

joined = (
    lines
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.age"), F.col("c.email"), F.col("c.credit_limit"))
)

enriched = (
    joined
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.city"))
)

matched_keys = (
    enriched
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2024-04-14")
        .select("customer_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "customer_id",
        "created_at",
        "code",
        "age",
        "email",
        "credit_limit",
        "city",
    )
    .orderBy(F.col("code").asc_nulls_last())
)
