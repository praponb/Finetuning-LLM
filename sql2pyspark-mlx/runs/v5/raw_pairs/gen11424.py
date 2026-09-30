from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

remaining_keys = (
    lines
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.customer_id", "src.created_at", F.col("p.age"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.age",
        F.col("d.country"),
        F.col("d.age").alias("customer_age"),
        F.col("d.name"),
    )
)

result = (
    joined
    .select("customer_id", "created_at", "age", "country", "name")
    .orderBy(F.col("country").asc_nulls_first())
)
