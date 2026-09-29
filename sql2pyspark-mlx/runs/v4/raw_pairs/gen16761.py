from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "order_id",
        "paid_at",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.first("amount").over(w2).alias("first_amount"),
    )
)
