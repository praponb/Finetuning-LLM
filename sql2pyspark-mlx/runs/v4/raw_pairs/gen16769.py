from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

base = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-01-23 11:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

result = (
    base
    .select(
        "*",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.ntile(3).over(w3).alias("amount_bucket"),
    )
)
