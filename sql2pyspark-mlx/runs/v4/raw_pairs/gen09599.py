from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.to_date(F.lit("2024-04-07")))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("clean_merchant", F.trim("merchant"))
)

scored = (
    filtered
    .withColumn("amount_max_90d", F.max("amount").over(w_running))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
    .withColumn("txn_type_count_amount", F.count("amount").over(w3))
    .withColumn("prev_amount", F.lag("amount", 1).over(w4))
    .withColumn("amount_avg_3", F.avg("amount").over(w5))
)

joined = (
    scored
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.status"))
)

result = (
    joined
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged_label",
        "clean_merchant",
        "amount_dense_rank",
        "txn_type_count_amount",
        F.when(F.col("merchant").rlike("^(Amazon|Lazada)$"), "GROUP_B").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "EXTENDED").otherwise("PRIORITY").alias("merchant_zone"),
        F.col("avg_transaction_amount"),
    )
    .orderBy(F.col("amount_dense_rank").asc_nulls_first())
)
