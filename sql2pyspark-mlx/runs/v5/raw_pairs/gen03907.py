from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy("paid_at", "id")

with_windows = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w).alias("last7_avg_amount"),
        F.max("amount").over(w).alias("last7_max_amount"),
        F.count("amount").over(w).alias("last7_count_amount"),
        F.min("amount").over(w).alias("last7_min_amount"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "last7_avg_amount",
        "last7_max_amount",
        "last7_count_amount",
        F.when(F.col("status").rlike("^(REFUNDED|SUCCESS)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy("amount")
)
