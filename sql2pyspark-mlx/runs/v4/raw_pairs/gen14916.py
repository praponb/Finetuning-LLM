from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.size("coupon_codes") > 1)
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.size("items").alias("num_items"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("num_items") >= 4, "HIGH").otherwise("LOW").alias("num_items_tier"),
    )
    .agg(
        F.max("num_items").alias("max_num_items"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("utm_source_or_default").alias("distinct_utm_source_or_defaults"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
