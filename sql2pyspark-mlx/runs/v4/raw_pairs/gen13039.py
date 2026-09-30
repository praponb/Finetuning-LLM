from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type", "merchant").orderBy("amount_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") < "2024-12-29"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

metrics = (
    sessions_transactions
    .select(
        "*",
        F.first("amount_int", ignorenulls=True).over(w3).alias("first_known_amount_int"),
        F.last("merchant", ignorenulls=True).over(w4).alias("last_known_merchant"),
    )
)

result = (
    metrics
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_int",
        "is_new_session",
        "session_no",
        F.when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "GROUP_B").when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "PRIORITY").otherwise("EXTENDED").alias("txn_type_zone"),
    )
    .orderBy("amount")
)
