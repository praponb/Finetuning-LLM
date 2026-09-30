from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("status").orderBy(F.col("amount").desc())

metrics = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w_recent).alias("amount_avg_5"),
        F.rank().over(w_part).alias("amount_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "amount_avg_5",
        "amount_rank",
        F.when(F.col("method").rlike("^(cash|paypal)$"), "CORE").otherwise("EXTENDED").alias("method_zone"),
    )
    .orderBy("method", F.col("order_id").desc())
)
