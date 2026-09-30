from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["7-Eleven", "Grab", "Starbucks"]))
    .select(
        "id",
        "txn_type",
        "amount",
        "account_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .select("id", "txn_type", "amount", "account_id")
    .orderBy("amount")
)
