from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("amount")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "amount",
        "order_date",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .select("id", "store_id", "amount", "order_date")
    .orderBy("id", "store_id")
)
