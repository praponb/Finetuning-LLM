from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")

result = (
    df_orders
    .filter(F.col("store_id") == 25)
    .select(
        "id",
        "channel",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
)
