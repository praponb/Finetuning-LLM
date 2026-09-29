from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "*",
        F.dense_rank().over(w_part).alias("amount_dense_rank"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)
