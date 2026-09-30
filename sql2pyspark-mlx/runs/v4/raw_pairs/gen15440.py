from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "amount",
        F.count("*").over(w).alias("order_id_count"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
