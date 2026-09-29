from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.lag("amount", 1, 0).over(w).alias("prev_amount"),
    )
    .orderBy(F.col("prev_amount").desc())
)
