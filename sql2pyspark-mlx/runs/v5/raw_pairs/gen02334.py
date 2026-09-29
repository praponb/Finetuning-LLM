from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .select("id", "account_id", "txn_type", "amount", "is_flagged", "is_new_session")
    .orderBy(F.col("amount").desc())
)
