from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id")
w4 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.first("merchant", ignorenulls=True).over(w2).alias("first_known_merchant"),
        F.ntile(10).over(w3).alias("amount_bucket"),
        F.sum("amount").over(w4).alias("amount_sum_30d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "first_known_merchant",
        "amount_bucket",
        F.when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "PRIORITY").otherwise("GROUP_A").alias("txn_type_zone"),
    )
)
