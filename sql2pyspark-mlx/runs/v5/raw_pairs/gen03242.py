from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w4 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.count("amount").over(w).alias("amount_count_30d"),
        F.row_number().over(w2).alias("row_seq"),
        F.lead("amount", 2).over(w3).alias("next_amount"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
