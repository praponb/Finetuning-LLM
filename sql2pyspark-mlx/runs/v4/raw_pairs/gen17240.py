from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("amount") > 1430)
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        (F.col("amount") - F.lag("amount", 2, 0).over(w)).alias("amount_change"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
