from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

exploded_carts = exploded.select("id", "qty", "price", F.explode("coupon_codes").alias("tag"))

matched_keys = (
    exploded_carts
    .select("id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

reconciled = (
    exploded_carts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("tag").isNotNull())
    .withColumn("qty_whole", F.floor("qty"))
    .withColumn("qty_whole_2", F.ceil("qty"))
)

with_windows = (
    cleaned
    .select(
        "qty",
        "price",
        "tag",
        "qty_whole",
        "qty_whole_2",
        F.sum("qty_whole_2").over(w).alias("tag_sum_qty_whole_2"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
    )
)

stats = (
    with_windows
    .withColumn("price_share_tier", F.when(F.col("price_share") >= 0.73, "PREMIUM").otherwise("STANDARD"))
    .cube("price_share_tier")
    .agg(
        F.percentile_approx("price_share", 0.5).alias("price_share_p50"),
        F.max(F.when(F.col("tag") == "VIP", F.col("qty_whole"))).alias("vip_max_qty_whole"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.count("*").alias("event_count"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("vip_max_qty_whole") < 1000000)
)

result = (
    stats
    .select(
        "price_share_tier",
        "price_share_p50",
        "tag_list",
        "qty_quartiles",
        "grouping_level",
    )
    .orderBy("price_share_tier")
)
