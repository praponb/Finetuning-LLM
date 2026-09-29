from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("discount") >= 0.24)
    .select(
        "id",
        "customer_id",
        "amount",
        "channel",
        "order_date",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
