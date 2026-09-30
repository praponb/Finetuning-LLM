from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("amount") <= 1900)
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
    )
)

result = (
    filtered
    .withColumn("amount_dense_rank", F.dense_rank().over(w))
    .withColumn("running_amount", F.sum("amount").over(w2))
)
