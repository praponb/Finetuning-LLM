from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("amount")

metrics = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select(
        "*",
        F.avg("amount").over(w).alias("last4_avg_amount"),
        F.min("amount").over(w).alias("last4_min_amount"),
        F.count("amount").over(w).alias("last4_count_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "last4_avg_amount",
        "amount_dense_rank",
    )
    .orderBy(F.col("paid_at").desc_nulls_last())
)
