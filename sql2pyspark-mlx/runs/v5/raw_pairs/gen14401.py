from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee")
w2 = Window.partitionBy("order_id", "product_id")
w3 = Window.orderBy("line_total")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("brand")

valid = (
    df_order_items
    .filter(F.col("order_id").isin([4, 5, 6]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

kept_keys = (
    valid
    .select("product_id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id") == 7)
        .select("product_id", "order_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.discount"), F.col("o.shipping_fee"), F.col("o.status"))
)

enriched = (
    with_dims
    .filter(F.col("unit_price") >= 250)
    .withColumn("shipping_fee_rank", F.rank().over(w))
    .withColumn("corr_unit_price_quantity", F.corr("unit_price", "quantity").over(w2))
)

enriched_order_items = (
    enriched
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        "src.line_total",
        "src.shipping_fee",
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("p.cost"),
    )
)

kept_keys_2 = (
    enriched_order_items
    .select("id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("quantity").isNull())
        .select("id", "product_id")
    )
)

kept_rows_order_items = (
    enriched_order_items
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows_order_items
    .select(
        "unit_price",
        "line_total",
        "shipping_fee",
        "launch_date",
        "brand",
        F.dense_rank().over(w3).alias("line_total_dense_rank"),
        F.avg("shipping_fee").over(w4).alias("order_id_avg_shipping_fee"),
        F.count("unit_price").over(w5).alias("brand_count_unit_price"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.percentile_approx("order_id_avg_shipping_fee", [0.25, 0.5, 0.75]).alias("order_id_avg_shipping_fee_quartiles"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("unit_price")).otherwise(0)).alias("initech_unit_price"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count("*").alias("event_count"),
    )
)
