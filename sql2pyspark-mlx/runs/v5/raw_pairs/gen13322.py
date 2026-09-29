from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "created_at",
        "key",
        F.array_contains("coupon_codes", "BUNDLE").alias("is_bundle"),
    )
)

matched_keys = (
    valid
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.array_contains("coupon_codes", "WELCOME5"))
        .select("customer_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.to_date("created_at") >= "2023-07-01")
        .select("customer_id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.email"), F.col("b.country"), F.col("b.name"))
)

base = (
    joined
    .filter((F.col("created_at") > F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .withColumn("name_title", F.initcap(F.trim("name")))
    .withColumn("resolved_country", F.coalesce("country", F.lit("OTHER")))
)

valid_carts = (
    base
    .filter(F.col("customer_id").isNotNull() & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "created_at",
        "key",
        "email",
        "country",
        "name",
        "name_title",
        "resolved_country",
        F.concat_ws(",", "email", "key").alias("email_key_key"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("resolved_key"),
    )
)

result = (
    valid_carts
    .groupBy("key", "country")
    .agg(
        F.count("*").alias("num_rows"),
        F.max("created_at").alias("ended_at"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy(F.col("country").desc_nulls_first())
)
