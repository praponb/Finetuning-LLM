from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

kept_keys = (
    exploded
    .select("customer_id", "id")
    .subtract(
        df_carts
        .filter(F.hour("created_at").between(8, 12))
        .select("customer_id", "id")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.credit_limit"))
)

result = (
    joined
    .groupBy(F.when(F.col("qty") >= 5, "LARGE").otherwise("SMALL").alias("qty_tier"))
    .agg(
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
    )
    .filter(F.col("avg_qty") >= 0)
    .orderBy(F.col("qty_tier").desc_nulls_last())
)
