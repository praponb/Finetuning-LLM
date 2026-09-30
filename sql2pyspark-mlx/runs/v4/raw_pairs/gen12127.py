from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.sum("amount").over(w_ordered).alias("cumulative_amount"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(5).over(w4).alias("amount_bucket"),
    )
)
