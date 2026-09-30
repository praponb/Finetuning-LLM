from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("method") == "paypal")
    .select(
        "id",
        "status",
        "method",
        "paid_at",
        "amount",
        "order_id",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
        F.row_number().over(w2).alias("row_num"),
    )
)
