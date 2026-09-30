from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "channel").orderBy("shipping_fee", "id")

metrics = (
    df_orders
    .select(
        "id",
        "store_id",
        "channel",
        F.lead("discount").over(w).alias("next_discount"),
        F.lag("discount").over(w).alias("prev_discount"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    metrics
    .select("id", "channel", "prev_shipping_fee", "seq_no")
    .orderBy(F.col("prev_shipping_fee").desc(), "channel")
)
