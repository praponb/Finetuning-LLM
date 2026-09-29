from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy("amount")

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        "status",
        "amount",
        F.sum("discount").over(w).alias("running_discount"),
        F.rank().over(w2).alias("amount_rank"),
    )
)
