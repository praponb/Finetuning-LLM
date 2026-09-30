from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id", "store_id").orderBy("amount_with_shipping", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-06").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "discount",
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        F.sum("discount").over(w).alias("cumulative_discount"),
        F.last("discount", ignorenulls=True).over(w_part).alias("last_known_discount"),
    )
)
