from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("merchant", "account_id").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "*",
        F.dense_rank().over(w_running).alias("amount_dense_rank"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.ntile(10).over(w4).alias("amount_bucket_2"),
        F.avg("amount").over(w5).alias("amount_avg_90d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "amount_dense_rank",
        "amount_bucket",
        "amount_bucket_2",
        "amount_avg_90d",
        F.when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "GROUP_B").when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("txn_type_zone"),
    )
)
