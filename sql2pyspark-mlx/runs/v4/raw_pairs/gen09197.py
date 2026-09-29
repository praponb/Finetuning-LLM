from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["hour_window", "activity_day"], ["hour_window"], []],
        "hour_window",
        "activity_day",
    )
    .agg(
        F.sum("price").alias("total_price"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.sum(F.when(F.col("promo") == "SPRING10", F.col("pos")).otherwise(0)).alias("spring10_pos"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_count"),
        F.max("created_at").alias("last_seen"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("spring10_count") >= 1)
    .orderBy(F.col("price_p50").asc_nulls_last())
)
