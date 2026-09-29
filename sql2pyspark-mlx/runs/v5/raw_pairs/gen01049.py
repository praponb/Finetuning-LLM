from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "promptpay", "bank_transfer"]))
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
    .orderBy("amount")
)
