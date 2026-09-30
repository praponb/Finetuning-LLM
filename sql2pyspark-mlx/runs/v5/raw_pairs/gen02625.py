from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc())
w2 = Window.partitionBy("channel")

result = (
    df_orders
    .select(
        "id",
        "status",
        "discount",
        "channel",
        "amount",
        F.rank().over(w).alias("discount_rank"),
        F.avg("amount").over(w2).alias("channel_avg_amount"),
    )
)
