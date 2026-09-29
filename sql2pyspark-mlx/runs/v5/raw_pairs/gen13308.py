from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "cash", "card"]))
    .select(
        "id",
        "status",
        "order_id",
        "amount",
        F.dense_rank().over(w).alias("amount_dense_rank"),
    )
)
