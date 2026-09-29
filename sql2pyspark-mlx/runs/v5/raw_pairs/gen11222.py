from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

joined = (
    exploded
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.credit_limit"), F.col("p.segment"))
)

filtered = (
    joined
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attr_key",
        "value",
        "segment",
        F.when(F.col("credit_limit") >= 14000, "High").when(F.col("credit_limit") >= 5200, "Medium").otherwise("Low").alias("credit_limit_band"),
        F.col("credit_limit").cast("int").alias("credit_limit_int"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.value",
        "e.segment",
        "e.credit_limit_int",
        F.col("p.email"),
        F.col("p.credit_limit"),
    )
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("credit_limit") >= 14800, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count("*").alias("row_count"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.max("credit_limit").alias("max_credit_limit_2"),
    )
    .filter(F.col("distinct_segments") >= 1)
)

result = (
    stats
    .select(
        "credit_limit_tier",
        "max_credit_limit",
        "corporate_credit_limit",
        "row_count",
        "distinct_segments",
        "max_credit_limit_2",
    )
    .orderBy("max_credit_limit")
)
