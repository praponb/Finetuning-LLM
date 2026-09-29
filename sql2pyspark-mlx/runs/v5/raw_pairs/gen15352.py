from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")
w2 = Window.partitionBy("method", "status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        "method",
        "status",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        (F.col("amount") - F.lag("amount", 1).over(w2)).alias("amount_change"),
    )
)
