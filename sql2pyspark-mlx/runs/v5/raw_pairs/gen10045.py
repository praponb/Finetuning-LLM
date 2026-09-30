from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")
w_ordered = Window.partitionBy("merchant", "txn_type").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "account_id").orderBy("amount_rounded", "id")

filtered = (
    df_transactions
    .filter(F.col("amount") > 2500)
    .withColumn("amount_rounded", F.round("amount", 1))
    .withColumn("merchant_txn_type_key", F.concat_ws("/", "merchant", "txn_type"))
)

result = (
    filtered
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        "amount_rounded",
        "merchant_txn_type_key",
        F.count("amount").over(w).alias("txn_type_count_amount"),
        F.lag("amount_rounded", 1).over(w2).alias("prev_amount_rounded"),
        F.sum("amount").over(w_ordered).alias("running_amount"),
        F.lead("amount", 1).over(w4).alias("next_amount"),
    )
)
