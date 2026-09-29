from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel")

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        "discount",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.avg("amount").over(w2).alias("channel_avg_amount"),
    )
)
