from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        F.ntile(3).over(w).alias("amount_bucket"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)
