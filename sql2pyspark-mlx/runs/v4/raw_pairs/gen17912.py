from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.orderBy("paid_at", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

with_windows = (
    df_payments
    .select(
        "*",
        F.lead("amount", 2).over(w_recent).alias("next_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
        F.last("amount", ignorenulls=True).over(w3).alias("last_known_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

result = (
    with_windows
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "next_amount",
        "amount_avg_5_prior",
        F.col("max_payment_amount"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
