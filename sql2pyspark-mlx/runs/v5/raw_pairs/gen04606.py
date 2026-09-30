from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("brand", "category").orderBy("stock_qty", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy(F.col("stock_qty_avg_5").desc_nulls_last())
w4 = Window.orderBy("last3_count_stock_qty", "id")
w5 = Window.partitionBy("supplier_id").orderBy("stock_qty_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        "resolved_brand",
        F.count("stock_qty").over(w_hist).alias("last3_count_stock_qty"),
        F.sum("stock_qty").over(w_hist).alias("last3_sum_stock_qty"),
        F.min("stock_qty").over(w_hist).alias("last3_min_stock_qty"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_5"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") <= "2025-04-01"), "left_anti")
)

scored = (
    kept
    .filter(F.col("stock_qty_avg_5").between(205, 280))
    .withColumn("stock_qty_avg_5_rank", F.rank().over(w_running))
    .withColumn("next_price", F.lead("price", 2).over(w4))
    .withColumn("cumulative_cost", F.sum("cost").over(w5))
)

with_dims = (
    scored
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.supplier_name"), F.col("p.rating"), F.col("p.country"))
)

eligible = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id") == 25), "left_anti")
)

totals = (
    eligible
    .groupBy("country")
    .agg(
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.round(F.sum("last3_min_stock_qty") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("last3_min_stock_qty_per_jp"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
)

result = totals.select("country", "last3_min_stock_qty_per_jp")
