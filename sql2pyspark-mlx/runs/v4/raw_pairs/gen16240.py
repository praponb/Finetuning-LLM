from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("avg_unit_price").desc())

enriched = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.amount"))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched_order_items = (
    deduped
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.stock_qty"), F.col("d.cost"), F.col("d.price"))
)

with_windows = (
    enriched_order_items
    .select(
        "*",
        F.avg("amount").over(w2).alias("amount_avg_2"),
        F.ntile(3).over(w3).alias("amount_bucket"),
    )
)

stats = (
    with_windows
    .groupBy(
        "order_id",
        F.when(F.col("amount_avg_2") >= 1209, "HIGH").otherwise("LOW").alias("amount_avg_2_tier"),
        "product_id",
    )
    .agg(
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .withColumn("avg_unit_price_dense_rank", F.dense_rank().over(w4))
)

result = (
    stats
    .select(
        "order_id",
        "amount_avg_2_tier",
        "product_id",
        "cost_p50",
        "quantity_bit_and",
        "avg_unit_price_dense_rank",
    )
    .orderBy(F.col("order_id").desc_nulls_first())
)
