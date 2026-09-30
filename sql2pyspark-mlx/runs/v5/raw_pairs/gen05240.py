from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

with_dims = (
    lines
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.name").isNotNull())
    .select("a.*", F.col("c.city"), F.col("c.age"), F.col("c.country"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.customer_id",
        "src.element",
        "src.city",
        "src.country",
        F.col("p.credit_limit"),
        F.col("p.segment"),
    )
)

with_dims_carts = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .select(
        "e.*",
        F.col("c.city").alias("customer_city"),
        F.col("c.age"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
    )
)

result = (
    with_dims_carts
    .select(
        "customer_id",
        "city",
        "country",
        "segment",
        "customer_city",
        "customer_credit_limit",
        F.when(F.col("city").rlike("^(Singapore|New York)$"), "PRIORITY").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first(), "segment")
)
