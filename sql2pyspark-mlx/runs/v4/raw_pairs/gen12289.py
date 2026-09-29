from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_transactions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

flagged = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w3))

joined = (
    sessionized
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.opened_date"), F.col("b.balance"), F.col("b.status"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.status").alias("account_status"))
)

pivoted = (
    with_dims
    .groupBy("status")
    .pivot("account_status", ["FROZEN", "OPEN", "CLOSED"])
    .agg(F.max("is_new_burst").alias("amount"))
    .select(
        "status",
        F.col("FROZEN").alias("frozen"),
        F.col("OPEN").alias("open"),
        F.col("CLOSED").alias("closed"),
    )
)

result = (
    pivoted
    .select(
        "status",
        "open",
        "closed",
        F.when(F.col("status").rlike("^(FROZEN|OPEN)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
)
