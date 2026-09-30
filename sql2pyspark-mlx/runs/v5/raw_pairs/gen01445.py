from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.partitionBy("status").orderBy("amount", "id")
w_running = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id")
w_running_2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w8 = Window.partitionBy("product_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("last_known_discount", "id")
w10 = Window.orderBy(F.col("product_id_avg_last_known_discount").desc(), "id")
w11 = Window.partitionBy("product_id").orderBy(F.col("prev_unit_price_2").desc_nulls_first())

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    latest
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.amount"), F.col("p.status"), F.col("p.discount"))
)

with_windows = (
    joined
    .filter(F.col("status").isin(["SHIPPED", "CANCELLED"]))
    .select(
        "id",
        "product_id",
        "unit_price",
        "amount",
        F.last("discount", ignorenulls=True).over(w2).alias("last_known_discount"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "product_id",
        "unit_price",
        "last_known_discount",
        "quantity_cume_dist",
        F.last("amount", ignorenulls=True).over(w_running).alias("last_known_amount"),
        F.avg("last_known_discount").over(w6).alias("product_id_avg_last_known_discount"),
        F.lag("unit_price", 2).over(w_running_2).alias("prev_unit_price"),
        F.sum("quantity_cume_dist").over(w8).alias("running_quantity_cume_dist"),
        F.lag("unit_price", 1).over(w9).alias("prev_unit_price_2"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "product_id",
        "last_known_discount",
        "quantity_cume_dist",
        "product_id_avg_last_known_discount",
        "prev_unit_price_2",
        F.ntile(5).over(w10).alias("product_id_avg_last_known_discount_bucket"),
        F.rank().over(w11).alias("prev_unit_price_2_rank"),
    )
)

totals = (
    scored
    .groupBy(
        F.when(F.col("prev_unit_price_2_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("prev_unit_price_2_rank_tier"),
    )
    .agg(
        F.avg("quantity_cume_dist").alias("avg_quantity_cume_dist"),
        F.round(F.avg("last_known_discount"), 2).alias("avg_last_known_discount"),
        F.percentile_approx("product_id_avg_last_known_discount", 0.5).alias("product_id_avg_last_known_discount_p50"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
    )
    .filter(F.col("distinct_product_ids") > 1)
)

result = (
    totals
    .select(
        "prev_unit_price_2_rank_tier",
        "avg_quantity_cume_dist",
        "avg_last_known_discount",
        "product_id_avg_last_known_discount_p50",
        "distinct_product_ids",
    )
    .orderBy(F.col("avg_quantity_cume_dist").desc())
)
