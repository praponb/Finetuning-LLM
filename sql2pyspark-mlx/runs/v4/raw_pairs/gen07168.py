from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 15))
    .select(
        "id",
        "method",
        "amount",
        "status",
        "paid_at",
        F.row_number().over(w).alias("row_num"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
    .orderBy(F.col("id").desc())
)
