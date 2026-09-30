from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "store_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
    .orderBy(F.col("id").desc())
)
