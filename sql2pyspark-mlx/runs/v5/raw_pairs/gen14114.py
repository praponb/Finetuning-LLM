from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("running_sum_stock_qty").desc())
w3 = Window.orderBy("running_sum_stock_qty")
w4 = Window.orderBy("running_sum_stock_qty", "id")
w5 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("stock_qty")
w7 = Window.orderBy(F.col("first_known_running_sum_stock_qty").desc(), "id")

joined = (
    df_order_items
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.product_name"), F.col("b.price"), F.col("b.stock_qty"))
)

enriched = (
    joined
    .withColumn("running_sum_stock_qty", F.sum("stock_qty").over(w_part))
    .withColumn("running_count_stock_qty", F.count("stock_qty").over(w_part))
    .withColumn("running_max_stock_qty", F.max("stock_qty").over(w_part))
    .withColumn("running_avg_stock_qty", F.avg("stock_qty").over(w_part))
)

deduped = (
    enriched
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "quantity",
        "price",
        "stock_qty",
        "running_sum_stock_qty",
        "running_avg_stock_qty",
    )
)

enriched_order_items = (
    deduped
    .select(
        "*",
        F.rank().over(w3).alias("running_sum_stock_qty_rank"),
        F.lag("running_sum_stock_qty", 1).over(w4).alias("prev_running_sum_stock_qty"),
        F.first("running_sum_stock_qty", ignorenulls=True).over(w5).alias("first_known_running_sum_stock_qty"),
    )
)

enriched_order_items_2 = (
    enriched_order_items
    .select(
        "*",
        F.rank().over(w6).alias("stock_qty_rank"),
        F.ntile(10).over(w7).alias("first_known_running_sum_stock_qty_bucket"),
    )
)

valid = (
    enriched_order_items_2
    .filter(F.col("stock_qty") >= 241)
    .select(
        "*",
        F.round((F.col("stock_qty_rank") * 0.9), 2).alias("stock_qty_rank_adjusted"),
    )
)

grouped = (
    valid
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_sum_stock_qty_tier", F.when(F.col("running_sum_stock_qty") >= 2210, "PREMIUM").otherwise("STANDARD"))
    .cube("quantity_tier", "running_sum_stock_qty_tier")
    .agg(
        F.percentile_approx("stock_qty_rank_adjusted", 0.5).alias("stock_qty_rank_adjusted_p50"),
        F.count("*").alias("event_count"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.sum("prev_running_sum_stock_qty").alias("total_prev_running_sum_stock_qty"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("stock_qty_rank_adjusted_p50") > 0)
)

result = (
    grouped
    .select(
        "running_sum_stock_qty_tier",
        "stock_qty_rank_adjusted_p50",
        "event_count",
        "quantity_bit_or",
        "total_prev_running_sum_stock_qty",
        "grouping_level",
    )
    .orderBy(F.col("quantity_bit_or").asc_nulls_first())
)
