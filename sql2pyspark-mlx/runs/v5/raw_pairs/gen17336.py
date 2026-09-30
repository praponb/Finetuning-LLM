from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.min("amount").over(w).alias("running_min_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.ntile(3).over(w2).alias("amount_bucket"),
    )
)
