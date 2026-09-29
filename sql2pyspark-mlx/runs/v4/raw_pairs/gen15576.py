from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w_running = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

with_windows = (
    df_shipments
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.sum("cost").over(w_running).alias("running_cost"),
    )
)

result = (
    with_windows
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "status",
        "cost_bucket",
        "running_cost",
        F.col("max_order_item_unit_price"),
    )
)
