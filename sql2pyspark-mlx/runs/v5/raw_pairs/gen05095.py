from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

flattened = (
    exploded
    .select("id", "customer_id", "key", F.explode_outer("coupon_codes").alias("code"))
)

kept_keys = (
    flattened
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("customer_id", "id")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("code").isin(["WELCOME5", "VIP"]))
    .withColumn("key_length", F.length("key"))
)

result = (
    filtered
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "key",
        "code",
        F.when(F.col("key").rlike("^(weight_kg|device)$"), "EXTENDED").otherwise("CORE").alias("key_zone"),
        F.col("max_subscription_monthly_fee"),
    )
)
