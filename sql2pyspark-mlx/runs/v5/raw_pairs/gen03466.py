from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

base = (
    exploded
    .filter(F.col("customer_id").isNotNull() & (F.to_date("created_at") < "2023-04-06"))
    .select(
        "*",
        F.when(F.col("value").isin(["WELCOME5", "BUNDLE"]), "Priority").otherwise("Other").alias("value_group"),
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label"),
    )
)

filtered = (
    base
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "value_group",
        "created_at_label",
        F.date_format("created_at", "yyyy-MM").alias("created_at_label_2"),
        F.coalesce("value", F.lit("OTHER")).alias("value_filled"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select(
        "s.customer_id",
        "s.created_at",
        "s.created_at_label",
        "s.created_at_label_2",
        F.col("b.credit_limit"),
        F.col("b.city"),
    )
)

result = (
    enriched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["country", "cart_city"], ["country"]],
        F.col("c.country").alias("country"),
        F.col("e.city").alias("cart_city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.credit_limit")).alias("max_credit_limit"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("country").asc_nulls_last())
)
