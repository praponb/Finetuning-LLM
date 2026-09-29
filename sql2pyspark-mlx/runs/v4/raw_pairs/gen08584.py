from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        F.percent_rank().over(w).alias("txn_time_pct_rank"),
    )
)
