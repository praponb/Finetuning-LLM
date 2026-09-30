from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", "coupon_codes", F.col("elem.product_id"))
)

flattened = exploded.select("id", "customer_id", F.explode("coupon_codes").alias("value"))

joined = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.id", "src.customer_id", F.col("d.email"), F.col("d.city"))
)

cleaned = (
    joined
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

result = (
    cleaned
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "email", "resolved_city", F.col("min_account_balance"))
    .orderBy(F.col("min_account_balance").desc_nulls_last(), "customer_id")
)
