from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id", "channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)
