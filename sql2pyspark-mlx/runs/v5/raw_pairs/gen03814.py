from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)
