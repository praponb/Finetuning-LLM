from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

lines = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "customer_id",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

enriched = (
    lines_carts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.promo",
        "s.tag",
        F.col("p.segment"),
        F.col("p.country"),
    )
)

base = (
    enriched
    .filter(F.col("country").isNotNull() & ~F.col("tag").isin(["WELCOME5", "VIP"]))
    .select(
        "id",
        "customer_id",
        "promo",
        "tag",
        "segment",
        "country",
        F.length("segment").alias("segment_length"),
        F.concat("country", F.lit("/"), "segment").alias("country_segment"),
    )
)

result = (
    base
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "segment_length",
        "country_segment",
        F.col("avg_subscription_monthly_fee"),
    )
)
