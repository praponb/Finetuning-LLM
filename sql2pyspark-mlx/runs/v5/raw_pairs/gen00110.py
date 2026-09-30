from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("merchant", "txn_type")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        F.ntile(5).over(w).alias("amount_bucket"),
        F.last("merchant", ignorenulls=True).over(w2).alias("last_known_merchant"),
        F.avg("amount").over(w3).alias("amount_avg_3_prior"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)
