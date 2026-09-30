from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id", "status").orderBy("amount", "id")
w2 = Window.partitionBy("store_id", "channel")

metrics = (
    df_orders
    .select(
        "*",
        F.lag("shipping_fee").over(w_hist).alias("prev_shipping_fee"),
        F.lead("discount").over(w_hist).alias("next_discount"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.corr("shipping_fee", "discount").over(w2).alias("corr_shipping_fee_discount"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "prev_shipping_fee",
        "next_discount",
        F.when(F.col("status").rlike("^(SHIPPED|PENDING)$"), "GROUP_B").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy("store_id")
)
