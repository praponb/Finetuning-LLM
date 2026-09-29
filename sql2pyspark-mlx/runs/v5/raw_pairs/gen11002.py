from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("method", "status").orderBy("amount", "id")

cleaned = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status_method_key",
        F.sum("amount").over(w).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
