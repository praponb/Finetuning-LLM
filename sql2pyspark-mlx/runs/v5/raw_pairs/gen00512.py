from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").asc_nulls_last(), "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_type") == "DEBIT"))
    .withColumn("txn_time_period", F.date_trunc("WEEK", "txn_time"))
    .withColumn("txn_time_label", F.date_format("txn_time", "MMM yyyy"))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "txn_time_period",
        "txn_time_label",
    )
)
