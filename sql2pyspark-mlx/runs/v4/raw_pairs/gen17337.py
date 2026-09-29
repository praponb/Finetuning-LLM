from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "paid_at",
        "amount",
        "order_id",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.row_number().over(w2).alias("amount_row"),
    )
    .orderBy(F.col("status").desc(), F.col("next_amount").desc())
)
