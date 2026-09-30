from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").asc_nulls_first())
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy(F.col("burst_id").desc())

ranked = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

with_dims = (
    sessions_transactions
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.is_flagged",
        "s.burst_id",
        F.col("d.account_type"),
    )
)

ranked_transactions = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "account_id", "is_flagged", "account_type")
)

joined = (
    ranked_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.account_type",
        F.col("p.balance"),
        F.col("p.account_type").alias("account_account_type"),
        F.col("p.opened_date"),
    )
)

result = joined.select("id", "account_id", "balance", "account_account_type", "opened_date")
