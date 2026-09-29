from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

enriched = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.country").isin(["DE", "US", "TH"]))
    .select(
        "src.id",
        "src.customer_id",
        "src.k",
        "src.value",
        F.col("d.segment"),
        F.col("d.credit_limit"),
        F.col("d.email"),
    )
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select(
        "id",
        "customer_id",
        "value",
        "segment",
        "email",
        F.upper("k").alias("k_upper"),
        F.coalesce("segment", F.lit("unassigned")).alias("segment_filled"),
        F.coalesce("k", F.lit("NONE")).alias("k_clean"),
    )
)

enriched_carts = (
    filtered
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.value",
        "a.segment",
        "a.k_upper",
        "a.segment_filled",
        F.col("b.segment").alias("customer_segment"),
    )
)

result = (
    enriched_carts
    .groupBy("customer_segment")
    .agg(
        F.count(F.when(F.col("customer_segment") == "Corporate", 1)).alias("corporate_count"),
        F.size(F.collect_set("customer_segment")).alias("num_customer_segments"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy("row_count")
)
