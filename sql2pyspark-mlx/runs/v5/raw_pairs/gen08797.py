from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

cleaned = (
    flattened
    .filter(F.col("customer_id").isNotNull())
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
)

joined = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select(
        "e.customer_id",
        "e.attr_key",
        "e.v",
        "e.coupon_codes_count",
        F.col("c.segment"),
        F.col("c.city"),
        F.col("c.name"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.customer_id",
        "s.attr_key",
        "s.v",
        "s.city",
        "s.name",
        F.col("p.signup_date"),
    )
)

joined_carts = (
    with_dims
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.customer_id",
        "src.attr_key",
        "src.city",
        "src.name",
        "src.signup_date",
        F.col("d.city").alias("customer_city"),
        F.col("d.age"),
    )
)

result = joined_carts.select("attr_key", "city", "name", "customer_city")
