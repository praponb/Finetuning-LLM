from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id")
w_grp = Window.orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy("merchant")

latest = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    latest
    .withColumn("amount_avg_7d", F.avg("amount").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("amount_rank", F.rank().over(w_grp))
)

scored = (
    with_windows
    .withColumn("seq_no_count_30d", F.count("seq_no").over(w5))
    .withColumn("merchant_max_seq_no", F.max("seq_no").over(w6))
)

filtered = (
    scored
    .filter((F.col("txn_time") >= F.lit("2023-07-09 21:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("seq_no_count_30d_str", F.col("seq_no_count_30d").cast("string"))
)

result = (
    filtered
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "dedup_rank",
        "amount_avg_7d",
        "seq_no",
        "amount_rank",
        "merchant_max_seq_no",
    )
)
