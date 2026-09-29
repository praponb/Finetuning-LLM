from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
    )
)
