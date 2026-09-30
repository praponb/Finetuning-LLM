from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        F.lag("amount", 2, 0).over(w).alias("prev_amount"),
    )
    .orderBy(F.col("paid_at").desc(), F.col("amount").desc())
)
