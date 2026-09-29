from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id").rowsBetween(-3, -1)
w_grp = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.avg("amount").over(w).alias("amount_avg_3_prior"),
        F.sum("amount").over(w_grp).alias("cumulative_amount"),
    )
)
