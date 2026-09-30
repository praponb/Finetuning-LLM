from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

remaining_keys = (
    exploded
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("customer_id", "id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("Gra%"))
    .select("e.id", "e.customer_id", "e.coupon_codes", "e.v", F.col("c.credit_limit"))
)

cleaned = (
    joined
    .filter(F.col("customer_id").isNotNull() & F.array_contains("coupon_codes", "WELCOME5"))
    .select("*", F.size("coupon_codes").alias("coupon_codes_count"))
)

base = (
    cleaned
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .withColumn("coupon_codes_count_adjusted", F.round((F.col("coupon_codes_count") * 1.07), 2))
    .withColumn("coupon_codes_count_per_credit_limit", F.round((F.col("coupon_codes_count") / F.col("credit_limit")), 2))
)

result = (
    base
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 13100, "LARGE").otherwise("SMALL"))
    .withColumn("coupon_codes_count_tier", F.when(F.col("coupon_codes_count") >= 71, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["credit_limit_tier", "coupon_codes_count_tier"], ["credit_limit_tier"], []],
        "credit_limit_tier",
        "coupon_codes_count_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count_2"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy("row_count_2")
)
