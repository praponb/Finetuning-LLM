from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at").desc())
w2 = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("order_id") == 20)
    .select(
        "id",
        "paid_at",
        "status",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("row_num"),
    )
)
