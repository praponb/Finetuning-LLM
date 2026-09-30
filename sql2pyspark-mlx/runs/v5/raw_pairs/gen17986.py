from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

matched_keys = (
    flattened
    .select("customer_id", "id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("customer_id", "id")
    )
)

verified = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select("s.*", F.col("b.email"), F.col("b.age"))
)

cleaned = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "tag",
        F.concat("email", F.lit(", "), "tag").alias("email_tag"),
    )
)

result = (
    cleaned
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .rollup("activity_day")
    .agg(
        F.max("created_at").alias("last_seen"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_count"),
        F.count_if(F.col("tag") == "SPRING10").alias("spring10_events"),
        F.min("created_at").alias("first_seen"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
