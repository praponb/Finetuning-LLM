from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity")
w4 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

enriched = (
    df_order_items
    .filter(F.col("order_id") == 37)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.avg("unit_price").over(w).alias("unit_price_avg_6"),
        F.avg("quantity").over(w2).alias("order_id_avg_quantity"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.quantity",
        F.col("d.order_date"),
        F.col("d.discount"),
        F.col("d.status"),
    )
)

with_dims_order_items = (
    with_dims
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.order_date",
        "s.discount",
        F.col("d.amount"),
        F.col("d.channel"),
    )
)

enriched_order_items = (
    with_dims_order_items
    .select(
        "id",
        "order_id",
        "order_date",
        "discount",
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.ntile(4).over(w5).alias("discount_bucket"),
    )
)

daily = (
    enriched_order_items
    .groupBy(
        F.when(F.col("discount") >= 0.41, "LARGE").otherwise("SMALL").alias("discount_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("discount_bucket", 0.95).alias("discount_bucket_p95"),
        F.avg("discount").alias("avg_discount"),
    )
    .filter(F.col("avg_discount") > 0)
)

result = (
    daily
    .select("discount_tier", "week_start", "discount_bucket_p95")
    .orderBy(F.col("week_start").desc(), "discount_bucket_p95")
)
