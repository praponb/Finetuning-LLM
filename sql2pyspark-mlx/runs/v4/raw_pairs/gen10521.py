from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.row_number().over(w).alias("amount_row"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy(F.col("dense_rnk").desc())
)
