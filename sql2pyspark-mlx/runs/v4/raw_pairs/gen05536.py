from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("least_quantity_unit_price_filled")
w2 = Window.orderBy(F.col("unit_price").desc_nulls_last())
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.max("quantity").alias("max_order_item_quantity"))

valid = (
    df_order_items
    .filter(F.col("unit_price") <= 170)
    .withColumn("unit_price_filled", F.coalesce("unit_price", F.lit(0)))
)

remaining_keys = (
    valid
    .select("product_id", "order_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id") == 6)
        .select("product_id", "order_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.least("quantity", "unit_price_filled").alias("least_quantity_unit_price_filled"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("least_quantity_unit_price_filled_dense_rank"),
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
    )
)

result = (
    enriched
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "unit_price_filled",
        "least_quantity_unit_price_filled",
        "least_quantity_unit_price_filled_dense_rank",
        "unit_price_cume_dist",
        F.col("max_order_item_quantity"),
    )
)
