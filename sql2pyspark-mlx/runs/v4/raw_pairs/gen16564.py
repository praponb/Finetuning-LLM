from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id")
w_ordered = Window.partitionBy("order_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.shipping_fee"), F.col("d.status"), F.col("d.order_date"))
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.min("quantity").over(w).alias("product_id_min_quantity"),
        F.sum("shipping_fee").over(w2).alias("order_id_sum_shipping_fee"),
        F.sum("quantity").over(w_ordered).alias("cumulative_quantity"),
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
    )
)

enriched_order_items_2 = (
    enriched_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.brand"), F.col("p.price"), F.col("p.launch_date"))
)

result = (
    enriched_order_items_2
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.avg(F.when(F.col("status") == "COMPLETED", F.col("price"))).alias("completed_avg_price"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("quantity_per_completed"),
    )
    .orderBy(F.col("quantity_per_completed").desc_nulls_last())
)
