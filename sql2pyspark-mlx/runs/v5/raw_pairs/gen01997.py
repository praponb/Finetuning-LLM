from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("channel", "status").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-02-25").cast("date")) & F.col("store_id").isin([13, 31, 39]))
    .select("*", F.upper("status").alias("status_upper"))
)

result = (
    filtered
    .filter(F.col("status") != "CANCELLED")
    .select(
        "id",
        "order_date",
        "amount",
        "shipping_fee",
        F.avg("shipping_fee").over(w_running).alias("shipping_fee_avg_6"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.last("shipping_fee", ignorenulls=True).over(w3).alias("last_known_shipping_fee"),
    )
)
