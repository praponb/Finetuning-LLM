from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

joined = (
    flattened
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.created_at",
        "src.tag",
        F.col("c.age"),
        F.col("c.credit_limit"),
    )
)

valid = (
    joined
    .filter(F.col("age") > 37)
    .withColumn("age_band", F.when(F.col("age") >= 66, "High").when(F.col("age") >= 28, "Medium").otherwise("Low"))
)

kept_keys = (
    valid
    .select("customer_id", "id")
    .intersectAll(
        df_carts
        .filter(F.col("customer_id").isin([1, 21, 36]))
        .select("customer_id", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & (~F.col("tag").eqNullSafe(F.lit("SPRING10"))))
    .select(
        "*",
        F.substring("tag", 1, 3).alias("tag_prefix"),
        F.hour("created_at").alias("created_at_hour"),
    )
)

result = (
    filtered
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        "age",
        "credit_limit",
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("credit_limit").desc_nulls_last())
)
