from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "status",
        "amount",
        "order_id",
        "paid_at",
        F.row_number().over(w).alias("rn"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_7"),
    )
)
