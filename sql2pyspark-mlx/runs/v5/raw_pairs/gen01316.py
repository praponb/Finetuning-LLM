from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "amount",
        "discount",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "amount", "discount")
    .orderBy("channel")
)
