from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at")
w2 = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2025-09-21")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("rnk_2"),
    )
    .orderBy("status")
)
