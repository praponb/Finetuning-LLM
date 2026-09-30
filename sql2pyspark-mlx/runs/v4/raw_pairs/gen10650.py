from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("product_id_plus_price").desc_nulls_last())
w4 = Window.orderBy("product_id_plus_price")
w5 = Window.partitionBy("value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w6 = Window.orderBy(F.col("product_id_plus_price").desc())

exploded = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

exploded_carts = (
    exploded
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "price",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

cleaned = (
    exploded_carts
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("customer_id").isNotNull() & F.col("value").isin(["WELCOME5", "SPRING10", "VIP"]))
    .select(
        "customer_id",
        "created_at",
        "product_id",
        "price",
        "value",
        (F.col("product_id") + F.col("price")).alias("product_id_plus_price"),
        F.coalesce("product_id", F.lit(0)).alias("product_id_filled"),
        F.coalesce("value", F.lit("DIRECT")).alias("resolved_value"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.max("product_id_plus_price").over(w).alias("product_id_plus_price_max_30m"),
        F.count("price").over(w2).alias("price_count_15m"),
        F.rank().over(w3).alias("product_id_plus_price_rank"),
    )
)

enriched = (
    metrics
    .withColumn("product_id_plus_price_dense_rank", F.dense_rank().over(w4))
    .withColumn("product_id_avg_1h", F.avg("product_id").over(w5))
    .withColumn("product_id_plus_price_percent_rank", F.percent_rank().over(w6))
)

base = (
    enriched
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 22:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("product_id_plus_price_minus_product_id_filled", (F.col("product_id_plus_price") - F.col("product_id_filled")))
    .withColumn("value_filled", F.coalesce("value", F.lit("UNKNOWN")))
)

kept_keys = (
    base
    .select("customer_id")
    .intersectAll(
        df_carts
        .filter(F.hour("created_at").between(6, 10))
        .select("customer_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("product_id_plus_price_rank") >= 9, "LARGE").otherwise("SMALL").alias("product_id_plus_price_rank_tier"),
        "value",
    )
    .agg(
        F.percentile_approx("product_id_plus_price_rank", 0.9).alias("product_id_plus_price_rank_p90"),
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.sum(F.when(F.col("value") == "WELCOME5", F.col("product_id_plus_price_minus_product_id_filled")).otherwise(0)).alias("welcome5_product_id_plus_price_minus_product_id_filled"),
        F.sum(F.when(F.col("value") == "VIP", F.col("product_id_filled"))).alias("vip_total_product_id_filled"),
        F.count_if(F.col("value") == "WELCOME5").alias("welcome5_events"),
        F.min("created_at").alias("started_at"),
    )
    .filter(F.col("welcome5_events") < 1)
    .orderBy(F.col("welcome5_events").asc_nulls_last())
)
