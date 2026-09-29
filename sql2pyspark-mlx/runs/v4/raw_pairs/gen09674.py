from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")

metrics = (
    df_transactions
    .select(
        "*",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.lag("amount", 1, 0).over(w2).alias("prev_amount_2"),
    )
)

result = (
    metrics
    .groupBy("txn_type", "merchant")
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.max("txn_time").alias("last_seen"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
    .filter(F.col("debit_amount") > 0)
)
