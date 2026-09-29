from pyspark.sql import functions as F

joined = (
    df_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.segment").isin(["Consumer", "Corporate"]))
    .select(
        "e.id",
        "e.customer_id",
        "e.order_date",
        "e.channel",
        "e.amount",
        "e.shipping_fee",
        F.col("p.segment"),
        F.col("p.credit_limit"),
    )
)

kept_keys = (
    joined
    .select("id", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("shipping_fee") < 22)
        .select("id", "customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined_orders = (
    verified
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.order_date",
        "e.amount",
        F.col("d.credit_limit").alias("customer_credit_limit"),
        F.col("d.city"),
    )
)

eligible = (
    joined_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([4, 25, 26, 37])), "left_anti")
)

with_dims = (
    eligible
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email"), F.col("p.signup_date"), F.col("p.credit_limit"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.age"), F.col("c.city").alias("customer_city"))
)

filtered = (
    enriched
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "customer_id",
        "amount",
        "customer_credit_limit",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        "customer_city",
        F.substring("customer_city", 1, 3).alias("customer_city_prefix"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "customer_credit_limit",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        "customer_city",
        F.when(F.col("city").rlike("^(Tokyo|Chiang Mai)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("customer_credit_limit").asc_nulls_last())
)
