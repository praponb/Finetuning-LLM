from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "amount",
        F.max("amount").over(w).alias("store_id_max_amount"),
    )
)
