from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")

result = (
    df_orders
    .select(
        "id",
        "amount",
        "store_id",
        F.first("store_id").over(w).alias("first_store_id"),
    )
)
