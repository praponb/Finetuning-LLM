from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") != 36)
    .select("a.*", F.col("c.email"), F.col("c.city"))
)

kept_keys = (
    with_dims
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.col("customer_id") == 32)
        .select("id", "customer_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.segment"), F.col("d.city").alias("customer_city"))
)

kept_keys_2 = (
    joined
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id", "customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined_carts = (
    verified
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.*",
        F.col("c.segment").alias("customer_segment"),
        F.col("c.email").alias("customer_email"),
        F.col("c.country"),
    )
)

result = (
    joined_carts
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "attr_key",
        "attr_value",
        "email",
        "city",
        "customer_city",
        "customer_segment",
        F.when(F.col("city").rlike("^(Tokyo|Berlin)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
        F.col("min_order_amount"),
    )
    .orderBy(F.col("customer_city").desc())
)
