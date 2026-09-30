from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
    )
)
