from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select("id", "customer_id", F.col("item.promo"))
)

remaining_keys = (
    flattened
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("customer_id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("customer_id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.customer_id", "e.promo", F.col("c.credit_limit"), F.col("c.segment"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.segment").alias("customer_segment"))
)

result = (
    joined
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.segment").alias("customer_segment"),
        F.col("src.promo").alias("promo"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.credit_limit")).alias("max_credit_limit"),
    )
)
