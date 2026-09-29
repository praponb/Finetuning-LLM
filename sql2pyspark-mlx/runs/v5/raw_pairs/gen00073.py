from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("status", "customer_id").orderBy("amount", "id")
w3 = Window.partitionBy("channel").orderBy("shipping_fee")
w4 = Window.partitionBy("channel").orderBy(F.col("amount").asc_nulls_last())

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        F.corr("shipping_fee", "amount").over(w).alias("corr_shipping_fee_amount"),
        F.lead("discount", 1).over(w2).alias("next_discount"),
        F.percent_rank().over(w3).alias("shipping_fee_percent_rank"),
        F.rank().over(w4).alias("amount_rank"),
    )
)
