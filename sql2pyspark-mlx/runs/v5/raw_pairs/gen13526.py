from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount", "id")
w2 = Window.orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        "discount",
        F.ntile(3).over(w).alias("discount_bucket"),
        F.lead("discount", 1).over(w2).alias("next_discount"),
    )
)
