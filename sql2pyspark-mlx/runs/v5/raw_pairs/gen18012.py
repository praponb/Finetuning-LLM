from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

enriched = (
    lines
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.name"), F.col("b.segment"), F.col("b.signup_date"))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select(
        "s.customer_id",
        "s.coupon_codes",
        "s.product_id",
        "s.qty",
        "s.name",
        "s.segment",
        "s.signup_date",
        F.col("p.email"),
    )
)

enriched_carts = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.is_active"))
    .select(
        "src.*",
        F.col("c.email").alias("customer_email"),
        F.col("c.segment").alias("customer_segment"),
    )
)

joined_carts = (
    enriched_carts
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.customer_id",
        "a.coupon_codes",
        "a.product_id",
        "a.signup_date",
        "a.email",
        "a.customer_segment",
        F.col("b.segment").alias("customer_segment_2"),
    )
)

joined_carts_2 = (
    joined_carts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.customer_id",
        "a.product_id",
        "a.customer_segment_2",
        F.col("d.age"),
        F.col("d.credit_limit"),
        F.col("d.country"),
    )
)

result = (
    joined_carts_2
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["customer_country", "city", "customer_segment_2"], ["customer_country", "city"], ["customer_country"]],
        F.col("d.country").alias("customer_country"),
        F.col("d.city").alias("city"),
        F.col("s.customer_segment_2").alias("customer_segment_2"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.credit_limit")).alias("max_credit_limit"),
        F.max(F.col("s.product_id")).alias("max_product_id"),
    )
)
