from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("greatest_line_total_quantity").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("greatest_line_total_quantity", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([5, 21, 32]))
    .withColumn("line_total", F.round((F.col("unit_price") / F.col("quantity")), 2))
)

base = (
    filtered
    .filter(F.col("quantity").between(4, 7))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        F.greatest("line_total", "quantity").alias("greatest_line_total_quantity"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.ntile(3).over(w).alias("greatest_line_total_quantity_bucket"),
        F.avg("line_total").over(w_grp).alias("line_total_avg_6"),
    )
)

enriched_order_items = (
    enriched
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.amount"), F.col("p.order_date"), F.col("p.status"))
)

daily = (
    enriched_order_items
    .groupBy("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.sum("unit_price") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("unit_price_per_returned"),
    )
    .filter(F.col("avg_quantity") >= 0)
)

result = (
    daily
    .select("status", "num_status", "avg_quantity", "unit_price_per_returned")
    .orderBy(F.col("unit_price_per_returned").desc())
)
