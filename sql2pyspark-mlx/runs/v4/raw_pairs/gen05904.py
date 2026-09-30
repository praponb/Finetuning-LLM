from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount")
w2 = Window.partitionBy("status").orderBy("discount", "id")
w3 = Window.orderBy("shipping_fee", "id")

scored = (
    df_orders
    .select(
        "*",
        F.percent_rank().over(w).alias("discount_percent_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.lag("discount", 1).over(w3).alias("prev_discount"),
    )
)

result = (
    scored
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "event_seq",
        "prev_discount",
        F.when(F.col("status").rlike("^(PENDING|RETURNED)$"), "EXTENDED").when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("amount").desc_nulls_last())
)
