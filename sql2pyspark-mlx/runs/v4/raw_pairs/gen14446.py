from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-11-01")
    .select(
        "id",
        "status",
        "amount",
        F.min("amount").over(w).alias("status_min_amount"),
    )
    .orderBy(F.col("amount").desc())
)
