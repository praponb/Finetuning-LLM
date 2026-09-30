from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "entry"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("item_no"),
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("price") >= 237, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("qty")).otherwise(0)).alias("bundle_qty"),
        F.avg(F.when(F.col("promo") == "BUNDLE", F.col("price"))).alias("bundle_avg_price"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
