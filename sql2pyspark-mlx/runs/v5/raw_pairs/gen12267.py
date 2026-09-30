from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2023-07-01")
    .select(
        "id",
        "method",
        "amount",
        F.min("amount").over(w).alias("method_min_amount"),
    )
)
