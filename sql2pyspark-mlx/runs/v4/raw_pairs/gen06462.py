from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")
w2 = Window.partitionBy("method")

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "method",
        "amount",
        F.dense_rank().over(w).alias("paid_at_dense_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
