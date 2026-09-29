from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")
w_running = Window.partitionBy("order_id").orderBy("unit_price_filled")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").asc_nulls_first())
w5 = Window.partitionBy("order_id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([3, 4, 27, 31]))
    .select("*", F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked
    .select("product_id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("product_id", "order_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("order_id").isin([26, 33]))
    .withColumn("unit_price_filled_rank", F.rank().over(w_running))
    .withColumn("product_id_avg_unit_price", F.avg("unit_price").over(w3))
)

scored = metrics.withColumn("unit_price_rank", F.rank().over(w4))

with_windows = (
    scored
    .select(
        "id",
        "product_id",
        "unit_price_filled",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.round((F.col("product_id_avg_unit_price") / F.sum("product_id_avg_unit_price").over(w5)), 4).alias("product_id_avg_unit_price_share"),
        F.sum("unit_price_filled_rank").over(w5).alias("order_id_sum_unit_price_filled_rank"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.product_id",
        "src.product_id_avg_unit_price_share",
        "src.order_id_sum_unit_price_filled_rank",
        F.col("b.brand"),
        F.col("b.stock_qty"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("product_id_avg_unit_price_share") >= 0.77, "HIGH").otherwise("LOW").alias("product_id_avg_unit_price_share_tier"),
    )
    .agg(
        F.percentile_approx("product_id_avg_unit_price_share", [0.25, 0.5, 0.75]).alias("product_id_avg_unit_price_share_quartiles"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("stock_qty_per_globex"),
        F.round(F.sum("order_id_sum_unit_price_filled_rank") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("order_id_sum_unit_price_filled_rank_per_stark"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
)
