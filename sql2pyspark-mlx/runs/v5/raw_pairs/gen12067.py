from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("method").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("method") != "cash")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.rank().over(w).alias("amount_rank"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
