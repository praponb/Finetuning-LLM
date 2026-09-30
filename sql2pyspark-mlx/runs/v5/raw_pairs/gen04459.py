from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "method",
        "paid_at",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.row_number().over(w2).alias("rn"),
    )
)
