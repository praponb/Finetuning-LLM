from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.row_number().over(w).alias("amount_row"),
        F.row_number().over(w2).alias("amount_row_2"),
    )
    .orderBy("amount_row")
)
