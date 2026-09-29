from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("supplier_id", "category")
w3 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_products
    .filter(F.col("cost") <= 180)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.corr("price", "cost").over(w2).alias("corr_price_cost"),
        F.last("stock_qty", ignorenulls=True).over(w3).alias("last_known_stock_qty"),
    )
)

filtered = (
    metrics
    .filter((F.col("corr_price_cost") > 0.5) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.abs("corr_price_cost").alias("abs_corr_price_cost"),
        (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([18, 24, 25, 31])), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.sum("abs_corr_price_cost").over(w4).alias("running_sum_abs_corr_price_cost"),
        F.max("abs_corr_price_cost").over(w4).alias("running_max_abs_corr_price_cost"),
        F.count("abs_corr_price_cost").over(w4).alias("running_count_abs_corr_price_cost"),
        F.min("abs_corr_price_cost").over(w4).alias("running_min_abs_corr_price_cost"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.supplier_name"), F.col("p.rating"), F.col("p.city"))
)

not_flagged_products = (
    enriched
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.launch_date")), "left_anti")
)

stats = (
    not_flagged_products
    .groupBy(
        F.date_trunc("week", "launch_date").alias("week_start"),
        F.date_trunc("month", "launch_date").alias("activity_month"),
        "category",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
    )
    .filter(F.col("num_rows") < 1000)
)

result = (
    stats
    .select("week_start", "activity_month", "category", "num_rows", "stock_qty_bit_or")
    .orderBy(F.col("num_rows").desc())
)
