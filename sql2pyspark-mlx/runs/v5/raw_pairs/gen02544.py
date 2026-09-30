from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "pos",
        F.col("entry.product_id"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

joined = (
    lines
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.product_id",
        F.col("d.age"),
        F.col("d.signup_date"),
        F.col("d.segment"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.signup_date").alias("customer_signup_date"),
        F.col("d.name"),
    )
)

joined_carts = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit").between(9000, 13900))
    .select(
        "a.customer_id",
        "a.product_id",
        "a.age",
        "a.segment",
        F.col("c.city"),
        F.col("c.name").alias("customer_name"),
        F.col("c.signup_date").alias("customer_signup_date_2"),
    )
)

result = (
    joined_carts
    .select("product_id", "segment", "customer_name", "customer_signup_date_2")
    .orderBy(F.col("customer_signup_date_2").desc_nulls_last())
)
