from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
    )
    .orderBy(F.col("prev_amount").desc(), F.col("order_id").desc())
)
