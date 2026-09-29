from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "amount",
        F.min("amount").over(w).alias("status_min_amount"),
        F.row_number().over(w2).alias("amount_row"),
    )
    .orderBy(F.col("id").desc())
)
