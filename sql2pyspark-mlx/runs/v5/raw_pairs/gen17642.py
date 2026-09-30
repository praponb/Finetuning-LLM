from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "attr_value",
        F.explode("coupon_codes").alias("tag"),
    )
)

filtered = (
    flattened_carts
    .filter(F.col("customer_id").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("utm_source"))))
    .withColumn("tag_title", F.initcap(F.trim("tag")))
)

enriched = (
    filtered
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.age"), F.col("p.city"))
)

scored = enriched.withColumn("age_sum_1h", F.sum("age").over(w))

base = (
    scored
    .filter(F.col("created_at") >= F.lit("2025-03-01 20:00:00").cast("timestamp"))
    .select(
        "*",
        F.round("age_sum_1h", 2).alias("age_sum_1h_rounded"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

kept_keys = (
    base
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.to_date("created_at") < "2023-12-23")
        .select("customer_id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "attr_value",
        "tag",
        "tag_title",
        "age",
        "city",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("tag").asc_nulls_first(), F.col("city").desc())
)
