from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

enriched = (
    lines
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.age"))
)

enriched_carts = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.country"),
        F.col("p.segment"),
        F.col("p.age").alias("customer_age"),
    )
)

totals = (
    enriched_carts
    .withColumn("age_tier", F.when(F.col("age") >= 58, "HIGH").otherwise("LOW"))
    .cube("age_tier", "segment")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select("age_tier", "segment", "distinct_ids", "duration_min", "sg_count", "gid")
)
