from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal", "promptpay"]))
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
)
