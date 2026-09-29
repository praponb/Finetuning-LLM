from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w_recent = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("account_id").isin([9, 14, 26, 30]))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        F.lead("amount", 1, 0).over(w).alias("next_amount"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("amount", ignorenulls=True).over(w_recent).alias("last_known_amount"),
    )
)
