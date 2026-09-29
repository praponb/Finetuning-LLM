from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("shipping_fee", "id")
w5 = Window.partitionBy("status").orderBy("delivered_date", "id")
w_running = Window.partitionBy("status").orderBy("shipping_fee_bucket", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("order_id_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.shipping_fee"), F.col("d.status").alias("order_status"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .select("a.*", F.col("o.order_date"), F.col("o.amount"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.max("amount").over(w).alias("order_id_max_amount"),
        F.avg("amount").over(w2).alias("amount_avg_2"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.ntile(10).over(w4).alias("shipping_fee_bucket"),
        F.lag("amount", 1).over(w5).alias("prev_amount"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.avg("cost").over(w_running).alias("cost_avg_6"),
        F.first("carrier", ignorenulls=True).over(w7).alias("first_known_carrier"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "shipping_fee",
        "order_status",
        "order_date",
        "amount",
        "order_id_max_amount",
    )
    .orderBy(F.col("order_status").desc_nulls_last())
)
