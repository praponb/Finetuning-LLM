from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

base = (
    flattened
    .filter(F.col("tag") != "VIP")
    .withColumn("tag_lower", F.lower("tag"))
)

joined = (
    base
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.id", "src.customer_id", F.col("p.name"))
)

with_dims = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .filter(F.col("d.credit_limit") <= 15300)
    .select("e.*", F.col("d.city"), F.col("d.credit_limit"))
)

remaining_keys = (
    with_dims
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "city",
        "credit_limit",
        F.when(F.col("city").rlike("^(Singapore|Bangkok)$"), "PRIORITY").otherwise("EXTENDED").alias("city_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy("customer_id")
)
