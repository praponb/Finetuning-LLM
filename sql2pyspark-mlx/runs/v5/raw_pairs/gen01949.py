from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("order_id")
w4 = Window.orderBy(F.col("corr_quantity_unit_price").desc_nulls_first())
w5 = Window.orderBy(F.col("corr_quantity_unit_price_2_scaled").desc_nulls_first())
w6 = Window.orderBy(F.col("corr_quantity_unit_price").desc_nulls_last())
w7 = Window.orderBy(F.col("quantity_share").desc())
w8 = Window.orderBy("corr_quantity_unit_price")

matched_keys = (
    df_order_items
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price").between(320, 330))
        .select("product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "id",
        "order_id",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price_2"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.count("unit_price").over(w).alias("product_id_count_unit_price"),
    )
)

base = (
    metrics
    .filter(F.col("corr_quantity_unit_price_2") >= 0.3)
    .select(
        "id",
        "corr_quantity_unit_price",
        "quantity_share",
        F.round((F.col("corr_quantity_unit_price_2") * 1.07), 2).alias("corr_quantity_unit_price_2_scaled"),
    )
)

valid = (
    base
    .filter(F.col("quantity_share") < 0.66)
    .withColumn("corr_quantity_unit_price_filled", F.coalesce("corr_quantity_unit_price", F.lit(0)))
    .withColumn("corr_quantity_unit_price_plus_corr_quantity_unit_price_2_scaled", (F.col("corr_quantity_unit_price") + F.col("corr_quantity_unit_price_2_scaled")))
)

metrics_order_items = (
    valid
    .withColumn("corr_quantity_unit_price_rank", F.rank().over(w4))
    .withColumn("corr_quantity_unit_price_2_scaled_cume_dist", F.cume_dist().over(w5))
)

with_windows = (
    metrics_order_items
    .withColumn("corr_quantity_unit_price_dense_rank", F.dense_rank().over(w6))
    .withColumn("quantity_share_cume_dist", F.cume_dist().over(w7))
    .withColumn("corr_quantity_unit_price_2_scaled_percent_rank", F.percent_rank().over(w5))
    .withColumn("corr_quantity_unit_price_cume_dist", F.cume_dist().over(w8))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("corr_quantity_unit_price_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("corr_quantity_unit_price_dense_rank_tier"),
    )
    .agg(
        F.max("quantity_share").alias("max_quantity_share"),
        F.count("*").alias("cnt"),
    )
)
