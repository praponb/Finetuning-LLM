from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id", "product_id")
w3 = Window.partitionBy("product_id", "order_id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_first())
w6 = Window.partitionBy("product_id", "order_id").orderBy("unit_price_avg_2", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy(F.col("unit_price_avg_2").desc())
w9 = Window.partitionBy("order_id")
w10 = Window.partitionBy("order_id", "status").orderBy("order_order_date", "id").rowsBetween(-5, -1)
w11 = Window.partitionBy("product_id").orderBy(F.col("quantity_percent_rank_avg_2").desc(), "id")
w12 = Window.partitionBy("product_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id", "order_id").orderBy("quantity_percent_rank_avg_2_bucket", "id").rowsBetween(-2, Window.currentRow)

joined = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.discount").between(0.14, 0.38))
    .select("e.*", F.col("d.shipping_fee"), F.col("d.order_date"))
)

metrics = (
    joined
    .withColumn("unit_price_avg_2", F.avg("unit_price").over(w))
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w2))
    .withColumn("product_id_count_shipping_fee", F.count("shipping_fee").over(w3))
    .withColumn("quantity_percent_rank", F.percent_rank().over(w4))
    .withColumn("unit_price_rank", F.rank().over(w5))
)

metrics_order_items = (
    metrics
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "shipping_fee",
        "order_date",
        "unit_price_avg_2",
        "order_id_avg_unit_price",
        "unit_price_rank",
        F.max("unit_price_rank").over(w6).alias("last7_max_unit_price_rank"),
        F.min("unit_price_rank").over(w6).alias("last7_min_unit_price_rank"),
        F.sum("unit_price_rank").over(w6).alias("last7_sum_unit_price_rank"),
        F.avg("quantity_percent_rank").over(w7).alias("quantity_percent_rank_avg_2"),
    )
)

joined_order_items = (
    metrics_order_items
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.unit_price",
        "a.order_date",
        "a.unit_price_avg_2",
        "a.order_id_avg_unit_price",
        "a.last7_max_unit_price_rank",
        "a.quantity_percent_rank_avg_2",
        F.col("b.status"),
        F.col("b.discount"),
        F.col("b.order_date").alias("order_order_date"),
    )
)

ranked = (
    joined_order_items
    .withColumn("dedup_rank", F.rank().over(w8))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .select(
        "id",
        "order_id",
        "product_id",
        "order_date",
        "last7_max_unit_price_rank",
        F.round((F.col("last7_max_unit_price_rank") / F.sum("last7_max_unit_price_rank").over(w9)), 4).alias("last7_max_unit_price_rank_share"),
        F.avg("last7_max_unit_price_rank").over(w10).alias("last7_max_unit_price_rank_avg_5_prior"),
        F.ntile(10).over(w11).alias("quantity_percent_rank_avg_2_bucket"),
        F.sum("quantity_percent_rank_avg_2").over(w12).alias("cumulative_quantity_percent_rank_avg_2"),
    )
)

with_windows = (
    enriched
    .withColumn("last3_min_last7_max_unit_price_rank_avg_5_prior", F.min("last7_max_unit_price_rank_avg_5_prior").over(w_ordered))
    .withColumn("last3_count_last7_max_unit_price_rank_avg_5_prior", F.count("last7_max_unit_price_rank_avg_5_prior").over(w_ordered))
    .withColumn("last3_max_last7_max_unit_price_rank_avg_5_prior", F.max("last7_max_unit_price_rank_avg_5_prior").over(w_ordered))
)

result = (
    with_windows
    .select(
        "id",
        "last7_max_unit_price_rank_share",
        "last7_max_unit_price_rank_avg_5_prior",
        "quantity_percent_rank_avg_2_bucket",
        "cumulative_quantity_percent_rank_avg_2",
        "last3_min_last7_max_unit_price_rank_avg_5_prior",
        "last3_count_last7_max_unit_price_rank_avg_5_prior",
        "last3_max_last7_max_unit_price_rank_avg_5_prior",
    )
    .orderBy(F.col("last3_max_last7_max_unit_price_rank_avg_5_prior").asc_nulls_last())
)
