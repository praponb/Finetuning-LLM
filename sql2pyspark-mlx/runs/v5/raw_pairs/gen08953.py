from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount")
w2 = Window.partitionBy("order_id")

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "order_id",
        F.percent_rank().over(w).alias("amount_pct_rank"),
        F.max("amount").over(w2).alias("order_id_max_amount"),
    )
)
