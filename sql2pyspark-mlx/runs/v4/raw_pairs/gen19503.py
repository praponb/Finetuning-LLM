from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .rollup("promo", "product_id")
    .agg(
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.max(F.when(F.col("promo") == "VIP", F.col("qty"))).alias("vip_max_qty"),
        F.grouping_id().alias("gid"),
    )
)
