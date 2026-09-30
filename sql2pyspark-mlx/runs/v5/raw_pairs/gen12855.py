from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .withColumn("next_amount", F.lead("amount", 1, 0).over(w))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w2))
)

result = (
    scored
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "next_amount",
        "first_known_merchant",
    )
)
