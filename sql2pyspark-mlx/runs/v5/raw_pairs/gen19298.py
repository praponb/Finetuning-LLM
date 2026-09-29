from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "amount",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
    )
)
