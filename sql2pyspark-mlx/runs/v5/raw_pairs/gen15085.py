from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.last("merchant", ignorenulls=True).over(w).alias("last_known_merchant"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
        F.max("amount").over(w3).alias("amount_max_30d"),
    )
)

result = (
    with_windows
    .select("id", "amount", "is_flagged", "last_known_merchant", "next_amount")
    .orderBy(F.col("next_amount").desc())
)
