from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.sum("amount").over(w_recent).alias("amount_sum_30d"),
    )
)
