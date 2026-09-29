from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .select("id", "account_id", "txn_type", "is_flagged")
    .orderBy(F.col("id").desc(), F.col("account_id").desc())
    .limit(10)
)
