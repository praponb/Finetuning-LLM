from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "status",
        "paid_at",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
    .orderBy("id", F.col("amount").desc())
)
