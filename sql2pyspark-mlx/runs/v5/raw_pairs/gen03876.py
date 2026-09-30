from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.price"),
    )
)

enriched = (
    exploded
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.age").between(53, 64))
    .select(
        "a.id",
        "a.customer_id",
        F.col("b.name"),
        F.col("b.signup_date"),
        F.col("b.country"),
    )
)

cleaned = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "name",
        "signup_date",
        F.coalesce("name", F.lit("unassigned")).alias("name_filled"),
    )
)

remaining_keys = (
    cleaned
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .cube("quarter_start")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count_2"),
        F.grouping_id().alias("gid"),
    )
)
