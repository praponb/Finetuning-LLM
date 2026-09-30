from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "account_id",
        "txn_time",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
    )
    .orderBy("amount")
)
