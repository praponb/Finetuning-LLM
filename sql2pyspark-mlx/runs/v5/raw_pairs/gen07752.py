from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_orders
    .select(
        "id",
        "status",
        "discount",
        "channel",
        "shipping_fee",
        "store_id",
        F.row_number().over(w).alias("rn"),
        F.last("store_id").over(w2).alias("last_store_id"),
    )
)
