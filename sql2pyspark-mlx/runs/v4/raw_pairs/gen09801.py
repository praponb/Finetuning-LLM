from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        "store_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "channel", "amount", "store_id")
)
