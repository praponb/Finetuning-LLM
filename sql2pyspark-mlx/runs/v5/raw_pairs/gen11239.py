from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        "txn_type",
        F.sum("amount").over(w).alias("running_amount"),
        F.rank().over(w2).alias("amount_rank"),
    )
)
