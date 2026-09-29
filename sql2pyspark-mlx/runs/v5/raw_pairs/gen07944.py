from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "session_id",
        F.when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "GROUP_A").when(F.col("txn_type").rlike("^(DEBIT|CREDIT)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("txn_type_zone"),
    )
    .orderBy(F.col("session_id").desc())
)
