from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last())

metrics = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)

result = metrics.select("id", "order_id", "paid_at", "status", "prev_amount", "row_seq")
