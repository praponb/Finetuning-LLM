from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-08-07")
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        (F.col("amount") - F.lag("amount", 1).over(w)).alias("amount_change"),
    )
    .orderBy(F.col("amount").desc())
)
