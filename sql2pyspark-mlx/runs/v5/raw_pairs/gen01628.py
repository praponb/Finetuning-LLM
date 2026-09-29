from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-07-01")
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.last("amount").over(w).alias("last_amount"),
    )
)
