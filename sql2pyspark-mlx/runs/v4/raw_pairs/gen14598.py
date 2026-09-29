from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_type",
        F.last("txn_type").over(w).alias("last_txn_type"),
    )
)
