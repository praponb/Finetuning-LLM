from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount_whole").desc(), "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-04-10").cast("date")) & ~F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.ceil("amount").alias("amount_whole"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

result = (
    cleaned
    .filter(F.col("account_id").isin([3, 16, 27]))
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        "is_flagged_label",
        F.sum("amount").over(w).alias("running_amount"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
