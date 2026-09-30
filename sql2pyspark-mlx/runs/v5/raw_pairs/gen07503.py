from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("shipping_fee", "id")
w_grp = Window.partitionBy("customer_id").orderBy("shipping_fee")

with_windows = (
    df_orders
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.dense_rank().over(w_grp).alias("shipping_fee_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "event_seq",
        "shipping_fee_dense_rank",
        F.when(F.col("channel").rlike("^(partner|store)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("channel_zone"),
    )
)
