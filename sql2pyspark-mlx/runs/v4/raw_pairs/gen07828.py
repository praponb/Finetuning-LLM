from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("li_item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
    )
)

flattened = (
    exploded
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

enriched = (
    flattened
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.segment"))
)

stats = (
    enriched
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .rollup("hour_window")
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.grouping("hour_window").alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "hour_window",
        "first_created_at",
        "tag_list",
        "segment_list",
        "grouping_id",
    )
    .orderBy("first_created_at")
)
