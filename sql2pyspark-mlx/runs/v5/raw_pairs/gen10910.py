from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("launch_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("price", "id")

deduped = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.price",
        "s.cost",
        "s.stock_qty",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([4, 31])), "left_semi")
)

scored = (
    eligible
    .select(
        "id",
        "price",
        "cost",
        "order_item_quantity",
        F.avg("stock_qty").over(w3).alias("running_avg_stock_qty"),
        F.count("stock_qty").over(w3).alias("running_count_stock_qty"),
        F.max("stock_qty").over(w3).alias("running_max_stock_qty"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

summary = (
    scored
    .cube(
        F.when(F.col("running_count_stock_qty") >= 17, "LARGE").otherwise("SMALL").alias("running_count_stock_qty_tier"),
        F.when(F.col("running_avg_stock_qty") >= 300, "PREMIUM").otherwise("STANDARD").alias("running_avg_stock_qty_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_max_stock_qty", 0.5).alias("running_max_stock_qty_p50"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("running_max_stock_qty", [0.25, 0.5, 0.75]).alias("running_max_stock_qty_quartiles"),
        F.round(F.avg("order_item_quantity"), 2).alias("avg_order_item_quantity"),
    )
)

result = (
    summary
    .select(
        "running_count_stock_qty_tier",
        "running_avg_stock_qty_tier",
        "distinct_ids",
        "running_max_stock_qty_p50",
        "distinct_ids_2",
        "avg_order_item_quantity",
    )
    .orderBy(F.col("distinct_ids_2").desc_nulls_last(), "running_count_stock_qty_tier")
)
