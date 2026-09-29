from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "brand")
w2 = Window.partitionBy("brand")
w3 = Window.orderBy(F.col("quantity").desc())
w4 = Window.partitionBy("brand").orderBy(F.col("corr_unit_price_quantity").desc())
w5 = Window.partitionBy("brand", "product_id").orderBy(F.col("quantity").asc_nulls_last())
w6 = Window.partitionBy("product_id", "order_id")
w7 = Window.partitionBy("order_id", "product_id").orderBy(F.col("brand_count_quantity").asc_nulls_last())
w8 = Window.partitionBy("order_id")
w_hist = Window.partitionBy("product_id", "order_id").orderBy(F.col("product_id_avg_brand_count_quantity").desc_nulls_last())
w_part = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity_dense_rank").desc_nulls_last())

remaining_keys = (
    df_order_items
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isin([14, 37]))
        .select("id", "product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.brand"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share_2"),
        F.corr("unit_price", "quantity").over(w2).alias("corr_unit_price_quantity"),
        F.dense_rank().over(w3).alias("quantity_dense_rank"),
    )
)

enriched_order_items = (
    with_windows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.product_name"))
)

enriched_order_items_2 = (
    enriched_order_items
    .withColumn("brand_count_quantity", F.count("quantity").over(w2))
    .withColumn("corr_unit_price_quantity_rank", F.rank().over(w4))
)

with_windows_order_items = (
    enriched_order_items_2
    .withColumn("quantity_rank", F.rank().over(w5))
    .withColumn("product_id_avg_brand_count_quantity", F.avg("brand_count_quantity").over(w6))
)

result = (
    with_windows_order_items
    .withColumn("brand_count_quantity_dense_rank", F.dense_rank().over(w7))
    .withColumn("order_id_count_quantity_share", F.count("quantity_share").over(w8))
    .withColumn("product_id_avg_brand_count_quantity_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("quantity_dense_rank_cume_dist", F.cume_dist().over(w_part))
)
