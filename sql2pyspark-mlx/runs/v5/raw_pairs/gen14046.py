from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("txn_time", "id")

with_windows = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.avg("amount").over(w).alias("amount_avg_6_prior"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_avg_6_prior",
        "first_known_amount",
        "prev_amount",
        F.when(F.col("merchant").rlike("^(Amazon|Grab)$"), "GROUP_B").when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
)
