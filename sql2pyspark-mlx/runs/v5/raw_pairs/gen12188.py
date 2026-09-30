from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "merchant",
        "txn_time",
        "account_id",
        F.first("account_id").over(w).alias("first_account_id"),
    )
)
