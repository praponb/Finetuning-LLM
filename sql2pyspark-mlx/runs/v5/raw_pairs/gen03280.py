from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "key",
        "attr_value",
        F.explode("coupon_codes").alias("code"),
    )
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.segment").isNotNull())
    .select("e.*", F.col("p.country"), F.col("p.email"))
)

kept_keys = (
    enriched
    .select("id", "customer_id")
    .exceptAll(
        df_carts
        .filter(F.col("customer_id").isNull())
        .select("id", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("customer_id")
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
    )
)
