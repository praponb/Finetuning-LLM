from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("paid_at").desc())

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.row_number().over(w).alias("amount_row"),
        F.percent_rank().over(w2).alias("paid_at_pct_rank"),
    )
)
