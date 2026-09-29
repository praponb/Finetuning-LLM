from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

joined = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.account_type") == "SAVINGS")
    .select("e.*", F.col("b.balance"))
)

enriched = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
    )
)

filtered = (
    enriched
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        "running_amount",
        "prev_amount",
        "amount_cume_dist",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.account_type").alias("account_type"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.amount")), 2).alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc_nulls_last())
)
