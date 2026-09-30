from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

valid = (
    exploded
    .filter(F.col("customer_id").isNotNull() & F.col("value").isNotNull())
    .select(
        "*",
        F.coalesce("value", F.lit("Unknown")).alias("value_filled"),
        F.when(F.col("value").isin(["SPRING10", "VIP"]), "Group A").otherwise("Other").alias("value_group"),
        F.coalesce("value", F.lit("DIRECT")).alias("resolved_value"),
    )
)

cleaned = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.col("value").eqNullSafe(F.lit("SPRING10"))))
    .withColumn("value_lower", F.lower("value"))
    .withColumn("value_filled_2", F.coalesce("value", F.lit("DIRECT")))
)

kept_keys = (
    cleaned
    .select("customer_id")
    .exceptAll(
        df_carts
        .filter(F.hour("created_at").between(11, 19))
        .select("customer_id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.signup_date"), F.col("b.credit_limit"), F.col("b.age"))
)

result = (
    joined
    .select(
        "customer_id",
        "value_filled",
        "value_group",
        "value_lower",
        "credit_limit",
        "age",
    )
    .orderBy("customer_id")
)
