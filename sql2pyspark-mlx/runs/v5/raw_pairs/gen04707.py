from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

flattened = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("k", "attr_value"))
)

with_dims = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.country") == "SG")
    .select("e.*", F.col("p.signup_date"), F.col("p.segment"), F.col("p.name"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "s.*",
        F.col("b.country"),
        F.col("b.email"),
        F.col("b.signup_date").alias("customer_signup_date"),
    )
)

valid = (
    joined
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "attr_value",
        "signup_date",
        "segment",
        "name",
        "country",
        "email",
        "customer_signup_date",
        F.when(F.col("k").isin(["weight_kg", "device"]), "Tier 1").otherwise("Other").alias("k_group"),
    )
)

result = (
    valid
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "attr_value",
        "signup_date",
        "segment",
        "name",
        "country",
        "email",
        "customer_signup_date",
        F.col("max_order_amount"),
    )
    .orderBy(F.col("customer_id").desc(), "signup_date")
)
