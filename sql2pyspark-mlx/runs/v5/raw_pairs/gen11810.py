from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("adjusted_amount", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("status")

base = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-07-14"))) & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.07), 2))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

result = (
    base
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        "adjusted_amount",
        "channel_clean",
        F.avg("adjusted_amount").over(w).alias("adjusted_amount_avg_2_prior"),
        F.corr("discount", "amount").over(w2).alias("corr_discount_amount"),
    )
)
