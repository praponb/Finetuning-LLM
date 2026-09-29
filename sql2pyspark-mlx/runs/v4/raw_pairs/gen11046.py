from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

enriched = (
    exploded
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.name"), F.col("d.email"))
)

kept_keys = (
    enriched
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.hour("created_at").between(7, 12))
        .select("customer_id", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_carts = (
    verified
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.city"), F.col("b.email").alias("customer_email"))
)

with_dims = (
    enriched_carts
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.country"), F.col("b.city").alias("customer_city"))
)

filtered = (
    with_dims
    .filter(F.col("email").like("%@yahoo.com"))
    .select(
        "id",
        "customer_id",
        "tag",
        "email",
        "city",
        "customer_email",
        "country",
        "customer_city",
        F.trim("email").alias("clean_email"),
        F.substring("tag", 1, 3).alias("tag_prefix"),
    )
)

result = (
    filtered
    .select(
        "id",
        "tag",
        "email",
        "city",
        "customer_email",
        "country",
        "customer_city",
        "tag_prefix",
    )
    .orderBy(F.col("customer_city").desc_nulls_last())
)
