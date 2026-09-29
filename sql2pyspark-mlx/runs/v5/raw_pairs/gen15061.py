from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id")
w4 = Window.orderBy(F.col("first_known_quantity").asc_nulls_last())
w_hist = Window.partitionBy("order_id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([5, 8, 37]))
    .select(
        "id",
        "order_id",
        "quantity",
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
)

with_windows = (
    filtered
    .filter(F.col("order_id").isin([10, 32]))
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w_grp).alias("first_known_quantity"),
        F.last("quantity", ignorenulls=True).over(w_ordered).alias("last_known_quantity"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price").between(150, 340))
        .select("id", "order_id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.status"))
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "quantity",
        "line_total",
        "last_known_quantity",
        F.sum("last_known_quantity").over(w3).alias("status_sum_last_known_quantity"),
        F.cume_dist().over(w4).alias("first_known_quantity_cume_dist"),
        F.stddev_samp("quantity").over(w_hist).alias("order_id_stddev_samp_quantity"),
        F.stddev_samp("last_known_quantity").over(w_hist).alias("order_id_stddev_samp_last_known_quantity"),
    )
)

enriched_order_items = (
    metrics
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.last_known_quantity",
        F.col("d.amount"),
        F.col("d.channel"),
    )
)

with_dims = (
    enriched_order_items
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.last_known_quantity",
        F.col("p.channel").alias("order_channel"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("last_known_quantity") >= 8, "HIGH").otherwise("LOW").alias("last_known_quantity_tier"),
        "order_channel",
    )
    .agg(
        F.count_if(F.col("order_channel") == "mobile").alias("mobile_events"),
        F.count(F.when(F.col("order_channel") == "partner", 1)).alias("partner_rows"),
        F.countDistinct("order_channel").alias("distinct_order_channels"),
        F.round(F.sum("last_known_quantity") / F.nullif(F.count(F.when(F.col("order_channel") == "mobile", 1)), F.lit(0)), 2).alias("last_known_quantity_per_mobile"),
        F.sort_array(F.collect_set("order_channel")).alias("order_channel_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("partner_rows") > 1)
    .orderBy("distinct_order_channels")
)
