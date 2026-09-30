from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-11-13").cast("date"))
    .select(
        "*",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.to_date("paid_at").alias("paid_at_date"),
    )
)

result = (
    filtered
    .filter(F.to_date("paid_at") >= "2024-04-01")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.avg("amount").over(w2).alias("amount_avg_3"),
    )
)
