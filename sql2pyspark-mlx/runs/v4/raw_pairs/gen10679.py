from pyspark.sql import functions as F

joined = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.channel"), F.col("o.status").alias("order_status"))
)

kept_keys = (
    joined
    .select("id")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id") == 11)
        .select("id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_window = (
    kept_rows
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.shipped_date") >= F.col("pr.starts_on")) & (F.col("e.shipped_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

daily = (
    with_window
    .groupBy(
        F.when(F.col("promo_id") >= 8, "LARGE").otherwise("SMALL").alias("promo_id_tier"),
        "carrier",
        "order_status",
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.count(F.when(F.col("order_status") == "SHIPPED", 1)).alias("shipped_rows"),
    )
)

result = (
    daily
    .select(
        "promo_id_tier",
        "carrier",
        "order_status",
        "discount_pct_quartiles",
        "shipped_rows",
    )
    .orderBy(F.col("shipped_rows").desc())
)
