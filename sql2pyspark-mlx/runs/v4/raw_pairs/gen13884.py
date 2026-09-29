from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "order_id",
        "amount",
        F.rank().over(w).alias("paid_at_rank"),
        F.percent_rank().over(w2).alias("amount_pct_rank"),
    )
    .orderBy("paid_at")
)
