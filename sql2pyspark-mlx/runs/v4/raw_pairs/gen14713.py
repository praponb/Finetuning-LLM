from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type", "merchant")
w3 = Window.orderBy(F.col("txn_time").desc(), "id")
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.concat_ws(",", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.trim("txn_type").alias("clean_txn_type"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w))
    .withColumn("txn_type_avg_amount", F.avg("amount").over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("cumulative_amount", F.sum("amount").over(w4))
    .withColumn("running_amount", F.sum("amount").over(w5))
)
