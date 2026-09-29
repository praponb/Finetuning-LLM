from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.dense_rank().over(w).alias("paid_at_dense_rank"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
