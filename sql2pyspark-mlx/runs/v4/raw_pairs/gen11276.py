from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

exploded = (
    lines
    .select(
        "customer_id",
        "created_at",
        "promo",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

valid = (
    exploded
    .filter(F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp"))
    .withColumn("promo_upper", F.upper("promo"))
    .withColumn("code_filled", F.coalesce("code", F.lit("OTHER")))
)

enriched = (
    valid
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"))
)

enriched_carts = (
    enriched
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country"), F.col("c.signup_date"))
)

enriched_carts_2 = (
    enriched_carts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.*",
        F.col("c.age"),
        F.col("c.email"),
        F.col("c.signup_date").alias("customer_signup_date"),
    )
)

result = (
    enriched_carts_2
    .groupBy(
        F.when(F.col("age") >= 67, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.when(F.col("credit_limit") >= 12700, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.sum("age").alias("total_age"),
    )
)
