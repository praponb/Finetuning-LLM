from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").desc())
w2 = Window.orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2024-12-01")
    .select(
        "id",
        "paid_at",
        "amount",
        F.dense_rank().over(w).alias("paid_at_dense_rank"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
