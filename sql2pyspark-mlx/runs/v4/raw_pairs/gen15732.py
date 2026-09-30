from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "method",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("paid_at_row"),
    )
)
