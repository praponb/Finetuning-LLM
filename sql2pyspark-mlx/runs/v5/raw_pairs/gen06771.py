from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

enriched = (
    lines
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.coupon_codes",
        "s.price",
        "s.promo",
        F.col("d.segment"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.customer_id", "s.price", F.col("c.country"))
)

valid = (
    joined
    .filter(F.col("customer_id").isNotNull() & (F.col("price") <= 220))
    .select(
        "*",
        F.floor("price").alias("price_whole"),
        F.round((F.col("price") * 1.07), 2).alias("price_adjusted"),
    )
)

joined_carts = (
    valid
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.credit_limit").between(6800, 15800))
    .select("src.*", F.col("p.city"))
)

result = (
    joined_carts
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "price", "price_whole", "city", F.col("min_account_balance"))
    .orderBy(F.col("price_whole").desc_nulls_first(), "price")
)
