from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy("txn_time", "id")
w3 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("is_new_burst", "id")

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-05-22")))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.trim("status").alias("clean_status"),
        F.abs("balance").alias("abs_balance"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.opened_date", "s.status", "s.abs_balance", F.col("c.txn_time"))
)

flagged = (
    with_last
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w3))

metrics = (
    sessionized
    .filter(F.year("opened_date") == 2025)
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w3))
    .withColumn("is_new_burst_max_7d", F.max("is_new_burst").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

result = (
    metrics
    .select(
        "id",
        "opened_date",
        "status",
        "abs_balance",
        "txn_time",
        "is_new_burst",
        "first_known_status",
        "is_new_burst_max_7d",
        "seq_no",
    )
    .orderBy(F.col("status").desc())
)
