from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_first(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("unit_price").asc_nulls_first())
w3 = Window.partitionBy("status")
w4 = Window.orderBy(F.col("quantity").asc_nulls_last())
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())

ranked = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

remaining_keys = (
    ranked
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([2, 21]))
        .select("order_id", "product_id")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.order_date"), F.col("p.status"), F.col("p.channel"))
)

metrics = (
    with_dims
    .select(
        "product_id",
        "quantity",
        "unit_price",
        "order_date",
        "channel",
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
        F.corr("unit_price", "quantity").over(w3).alias("corr_unit_price_quantity"),
        F.rank().over(w4).alias("quantity_rank"),
        F.round((F.col("quantity") / F.sum("quantity").over(w5)), 4).alias("quantity_share"),
    )
)

scored = (
    metrics
    .select(
        "quantity",
        "order_date",
        "unit_price_cume_dist",
        "corr_unit_price_quantity",
        "quantity_share",
        F.max("corr_unit_price_quantity").over(w6).alias("product_id_max_corr_unit_price_quantity"),
        F.cume_dist().over(w7).alias("unit_price_cume_dist_2"),
    )
)

daily = (
    scored
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.max("unit_price_cume_dist").alias("max_unit_price_cume_dist"),
        F.percentile_approx("quantity_share", [0.25, 0.5, 0.75]).alias("quantity_share_quartiles"),
    )
    .filter(F.col("max_unit_price_cume_dist") >= 0)
)

result = daily.select("week_start", "event_count", "quantity_share_quartiles")
