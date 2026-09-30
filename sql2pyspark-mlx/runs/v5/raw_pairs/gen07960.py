from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.promo"),
    )
)

base = (
    lines
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isin([16, 17, 27, 40]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "product_id",
        "promo",
        F.trim("promo").alias("clean_promo"),
        F.coalesce("promo", F.lit("DIRECT")).alias("promo_filled"),
    )
)

base_carts = (
    base
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "product_id",
        "clean_promo",
        "promo_filled",
        F.date_trunc("month", "created_at").alias("created_at_period"),
    )
)

kept_keys = (
    base_carts
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.col("created_at") >= "2024-07-01")
        .select("customer_id", "id")
    )
)

reconciled = (
    base_carts
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "created_at_period").alias("activity_month"),
    )
    .agg(
        F.min("created_at_period").alias("first_created_at_period"),
        F.min("created_at_period").alias("started_at"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
    )
    .filter(F.col("avg_product_id") > 0)
    .orderBy(F.col("product_id").desc())
)
