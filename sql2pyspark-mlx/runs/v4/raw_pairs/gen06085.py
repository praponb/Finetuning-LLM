from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "amount",
        "txn_time",
        F.row_number().over(w).alias("rn"),
        F.percent_rank().over(w2).alias("txn_time_pct_rank"),
    )
)
