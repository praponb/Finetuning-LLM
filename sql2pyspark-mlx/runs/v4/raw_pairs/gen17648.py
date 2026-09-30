from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([10, 13, 31, 40]))
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
    )
    .orderBy("id")
)
