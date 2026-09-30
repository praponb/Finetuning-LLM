from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

with_dims = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.age"), F.col("d.email"), F.col("d.name"))
)

cleaned = (
    with_dims
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "qty",
        "price",
        "promo",
        "age",
        "email",
        F.length("email").alias("email_length"),
        (F.col("price") - F.col("product_id")).alias("price_minus_product_id"),
    )
)

kept_keys = (
    cleaned
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "BUNDLE"))
        .select("customer_id", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    kept_rows
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .groupingSets(
        [["customer_id", "hour_window"], ["customer_id"], []],
        "customer_id",
        "hour_window",
    )
    .agg(
        F.count_if(F.col("promo") == "SPRING10").alias("spring10_events"),
        F.count("*").alias("cnt"),
        F.percentile_approx("email_length", [0.25, 0.5, 0.75]).alias("email_length_quartiles"),
    )
    .filter(F.col("spring10_events") > 1)
)

result = (
    totals
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "hour_window",
        "cnt",
        "email_length_quartiles",
        F.col("max_account_balance"),
    )
)
