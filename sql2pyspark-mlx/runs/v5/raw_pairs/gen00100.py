from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("channel").orderBy("amount", "id")
w2 = Window.partitionBy("channel", "store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-10-04").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "channel",
        "shipping_fee",
        "channel_status_key",
        F.lag("discount").over(w_grp).alias("prev_discount"),
        F.lead("shipping_fee").over(w_grp).alias("next_shipping_fee"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
    )
)
