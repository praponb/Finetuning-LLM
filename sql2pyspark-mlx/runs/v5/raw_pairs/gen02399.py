from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

filtered = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01 23:00:00").cast("timestamp")) & F.col("qty").between(2, 4))
    .select("*", F.round("product_id", 2).alias("product_id_rounded"))
)

base = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & (F.col("product_id") > 6))
    .select("*", F.dayofmonth("created_at").alias("created_at_day"))
)

base_carts = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp")) & (F.col("promo").eqNullSafe(F.lit("WELCOME5"))))
    .withColumn("promo_upper", F.upper("promo"))
    .withColumn("promo_filled", F.coalesce("promo", F.lit("UNKNOWN")))
)

base_carts_2 = (
    base_carts
    .filter((F.col("created_at") >= F.lit("2025-03-01 15:00:00").cast("timestamp")) & F.col("promo").isNotNull())
    .withColumn("product_id_plus_created_at_day", (F.col("product_id") + F.col("created_at_day")))
    .withColumn("resolved_promo", F.coalesce("promo", F.lit("UNKNOWN")))
)

matched_keys = (
    base_carts_2
    .select("customer_id", "id")
    .intersect(
        df_carts
        .filter(F.col("customer_id") == 21)
        .select("customer_id", "id")
    )
)

kept_rows = (
    base_carts_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base_carts_3 = (
    kept_rows
    .filter(F.col("created_at") >= F.lit("2025-03-01 19:00:00").cast("timestamp"))
    .select(
        "customer_id",
        "promo",
        "created_at_day",
        "promo_upper",
        "promo_filled",
        "product_id_plus_created_at_day",
        F.coalesce("promo", F.lit("unassigned")).alias("promo_filled_2"),
    )
)

result = (
    base_carts_3
    .select(
        "customer_id",
        "promo",
        "created_at_day",
        "promo_upper",
        "promo_filled",
        "product_id_plus_created_at_day",
        F.when(F.col("promo").rlike("^(SPRING10|VIP)$"), "PRIORITY").when(F.col("promo").rlike("^(BUNDLE|WELCOME5)$"), "OTHER_ZONE").otherwise("CORE").alias("promo_zone"),
    )
    .orderBy("product_id_plus_created_at_day")
)
