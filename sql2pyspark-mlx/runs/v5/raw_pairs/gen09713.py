from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("unit_price").asc_nulls_first())
w_hist = Window.partitionBy("product_id")

joined = (
    df_order_items
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.product_name"))
)

kept_keys = (
    joined
    .select("id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") == 8)
        .select("id", "order_id")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.status"), F.col("o.shipping_fee"), F.col("o.channel"))
)

with_dims_order_items = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.launch_date"),
        F.col("p.product_name").alias("product_product_name"),
    )
)

with_windows = (
    with_dims_order_items
    .select(
        "quantity",
        "product_name",
        "channel",
        "launch_date",
        F.percent_rank().over(w_grp).alias("unit_price_percent_rank"),
        F.sum("shipping_fee").over(w_hist).alias("product_id_sum_shipping_fee"),
    )
)

grouped = (
    with_windows
    .rollup(
        F.when(F.col("unit_price_percent_rank") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("unit_price_percent_rank_tier"),
        F.date_trunc("week", "launch_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("quantity_per_store"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("product_id_sum_shipping_fee", 0.5).alias("product_id_sum_shipping_fee_p50"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("product_id_sum_shipping_fee"))).alias("mobile_total_product_id_sum_shipping_fee"),
    )
    .filter(F.col("quantity_bit_or") < 1000)
)

result = (
    grouped
    .select(
        "unit_price_percent_rank_tier",
        "week_start",
        "quantity_bit_or",
        "product_id_sum_shipping_fee_p50",
        "store_count",
        "mobile_total_product_id_sum_shipping_fee",
    )
    .orderBy(F.col("unit_price_percent_rank_tier").desc())
)
