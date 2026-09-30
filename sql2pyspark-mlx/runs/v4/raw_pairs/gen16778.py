from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("amount", "id")

with_windows = (
    df_transactions
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("prev_amount", F.lag("amount", 1).over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "amount",
        "merchant",
        "seq_no",
        "prev_amount",
        F.when(F.col("merchant").rlike("^(Shell|Amazon)$"), "GROUP_B").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("merchant_zone"),
    )
)
