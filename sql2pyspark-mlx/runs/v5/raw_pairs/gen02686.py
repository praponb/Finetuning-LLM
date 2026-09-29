from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

exploded = (
    lines
    .select(
        "id",
        "customer_id",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

enriched = (
    exploded
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.credit_limit") <= 9700)
    .select("e.*", F.col("b.country"), F.col("b.credit_limit"), F.col("b.city"))
)

enriched_carts = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.customer_id",
        "s.price",
        "s.country",
        "s.credit_limit",
        F.col("p.city").alias("customer_city"),
    )
)

result = (
    enriched_carts
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 13400, "HIGH").otherwise("LOW"))
    .rollup("credit_limit_tier", "customer_id")
    .agg(
        F.max(F.when(F.col("country") == "US", F.col("price"))).alias("us_max_price"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.count(F.when(F.col("customer_city") == "Berlin", 1)).alias("berlin_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("num_rows") > 1)
)
