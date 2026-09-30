from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("quantity_rounded", "id")
w_ordered = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([18, 36]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

scored = (
    cleaned
    .filter(F.col("product_id") == 2)
    .select(
        "id",
        "order_id",
        F.ntile(5).over(w_running).alias("quantity_rounded_bucket"),
        F.sum("quantity_rounded").over(w_ordered).alias("running_quantity_rounded"),
        F.sum("unit_price").over(w_grp).alias("running_unit_price"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.quantity_rounded_bucket",
        F.col("b.shipping_fee"),
        F.col("b.discount"),
        F.col("b.channel"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.discount",
        F.col("o.shipping_fee").alias("order_shipping_fee"),
        F.col("o.order_date"),
    )
)

with_dims_order_items = (
    joined
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.discount",
        "s.order_date",
        F.col("p.shipping_fee"),
        F.col("p.status"),
        F.col("p.discount").alias("order_discount"),
    )
)

summary = (
    with_dims_order_items
    .groupBy(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") >= 1)
)

result = (
    summary
    .select("year_start", "activity_month", "num_status")
    .orderBy(F.col("num_status").desc())
)
