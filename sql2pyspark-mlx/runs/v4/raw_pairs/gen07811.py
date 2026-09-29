from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        "status",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
    )
    .orderBy("next_amount")
)
