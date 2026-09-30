from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("account_id")
w3 = Window.partitionBy("txn_type")
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.rank().over(w).alias("amount_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share_2"),
        F.sum("amount").over(w4).alias("running_amount"),
    )
)
