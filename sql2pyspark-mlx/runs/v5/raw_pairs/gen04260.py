from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("line_total").desc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.orderBy(F.col("unit_price_share").desc_nulls_first())

kept_keys = (
    df_order_items
    .select("order_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") != 5)
        .select("order_id", "id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("order_id").isin([1, 35]))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

metrics = (
    cleaned
    .filter(F.col("order_id").isin([19, 40]))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4))
    .withColumn("line_total_percent_rank", F.percent_rank().over(w2))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w3))
)

enriched = (
    metrics
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "line_total",
        "line_total_percent_rank",
        F.dense_rank().over(w4).alias("unit_price_share_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "line_total_percent_rank",
        "unit_price_share_dense_rank",
    )
    .orderBy(F.col("unit_price").asc_nulls_last())
)
