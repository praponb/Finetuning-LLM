from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")
w2 = Window.orderBy(F.col("qty").asc_nulls_last())

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

lines = (
    flattened
    .select(
        "product_id",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

valid = (
    lines
    .filter(F.col("promo").isNotNull() & (F.col("price") >= 200))
    .withColumn("adjusted_price", F.round((F.col("price") * 100), 2))
)

valid_carts = (
    valid
    .filter(F.col("value").isin(["BUNDLE", "VIP", "SPRING10"]))
    .select(
        "product_id",
        "qty",
        "price",
        "promo",
        "value",
        "adjusted_price",
        (F.col("price") + F.col("qty")).alias("price_plus_qty"),
    )
)

valid_carts_2 = (
    valid_carts
    .filter(F.col("price_plus_qty") <= 36)
    .select(
        "qty",
        "price",
        "value",
        "adjusted_price",
        "price_plus_qty",
        (F.col("product_id") + F.col("price")).alias("product_id_plus_price"),
    )
)

metrics = (
    valid_carts_2
    .select(
        "*",
        F.max("price").over(w).alias("value_max_price"),
        F.max("product_id_plus_price").over(w).alias("value_max_product_id_plus_price"),
        F.cume_dist().over(w2).alias("qty_cume_dist"),
    )
)

valid_carts_3 = (
    metrics
    .filter(F.col("value").isNotNull() & (F.col("product_id_plus_price") < 22))
    .select(
        "qty",
        "price",
        "value",
        "adjusted_price",
        "price_plus_qty",
        "product_id_plus_price",
        "value_max_price",
        "value_max_product_id_plus_price",
        "qty_cume_dist",
        F.ceil("value_max_price").alias("value_max_price_whole"),
        F.coalesce("value", F.lit("N/A")).alias("value_clean"),
    )
)

daily = (
    valid_carts_3
    .groupBy("value")
    .agg(
        F.size(F.collect_set("value")).alias("num_values"),
        F.count_if(F.col("value") == "WELCOME5").alias("welcome5_events"),
        F.percentile_approx("value_max_price", [0.25, 0.5, 0.75]).alias("value_max_price_quartiles"),
        F.percentile_approx("product_id_plus_price", [0.25, 0.5, 0.75]).alias("product_id_plus_price_quartiles"),
    )
)

result = (
    daily
    .select(
        "value",
        "value_max_price_quartiles",
        "product_id_plus_price_quartiles",
        F.when(F.col("value").rlike("^(VIP|SPRING10)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("value_zone"),
    )
    .orderBy(F.col("value_zone").desc_nulls_first(), "value")
)
