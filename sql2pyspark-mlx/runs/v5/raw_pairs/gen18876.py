from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines_carts = lines.select("id", "product_id", "qty", F.explode("coupon_codes").alias("value"))

cleaned = (
    lines_carts
    .filter(F.col("value").isNotNull())
    .withColumn("qty_adjusted", F.round((F.col("qty") * 1.1), 2))
    .withColumn("resolved_value", F.coalesce("value", F.lit("OTHER")))
)

filtered = (
    cleaned
    .filter(F.col("value").isNotNull() & (F.col("value").eqNullSafe(F.lit("VIP"))))
    .withColumn("value_group", F.when(F.col("value").isin(["VIP", "BUNDLE"]), "Priority").otherwise("Other"))
    .withColumn("product_id_tier", F.when(F.col("product_id") >= 8, "High").when(F.col("product_id") >= 4, "Medium").otherwise("Low"))
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id") == 21)
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("value").isNotNull() & F.col("value").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("value")).alias("value_title"),
        F.col("qty_adjusted").cast("int").alias("qty_adjusted_int"),
        F.coalesce("value", F.lit("DIRECT")).alias("resolved_value_2"),
    )
)

valid = (
    base
    .filter(F.col("product_id").isNotNull())
    .withColumn("least_qty_qty_adjusted_int", F.least("qty", "qty_adjusted_int"))
)

result = (
    valid
    .groupBy("value")
    .agg(
        F.percentile_approx("product_id", 0.5).alias("product_id_p50"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy("product_id_p50")
)
