from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((~F.col("merchant").eqNullSafe(F.lit("Starbucks"))) & (F.col("txn_type") == "FEE"))
    .select(
        "*",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

with_prev = base.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "txn_time_label",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

joined = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select(
        "a.*",
        F.col("p2.status"),
        F.col("p2.account_type"),
        F.col("p2.opened_date"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.account_type").alias("account_account_type"))
)

result = (
    with_dims
    .select(
        "id",
        "txn_time",
        "merchant",
        "txn_time_label",
        "session_no",
        "status",
        "opened_date",
        "account_account_type",
    )
    .orderBy(F.col("status").desc(), F.col("id").desc())
    .limit(10)
)
