from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy(F.col("txn_time").desc(), "id")
w4 = Window.partitionBy("merchant").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("session_no", "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-10-01"))) & F.col("merchant").isNotNull())
    .withColumn("txn_type_lower", F.lower("txn_type"))
)

valid_transactions = (
    valid
    .filter((F.col("txn_time") >= F.lit("2024-05-12 06:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & F.col("merchant").isin(["Shell", "Lazada"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_type_lower",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
    )
)

flagged = (
    valid_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

metrics = (
    sessions_transactions
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("running_is_new_session", F.sum("is_new_session").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

result = (
    metrics
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_type_lower",
        "merchant_title",
        "session_no",
        F.when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(Amazon|Lazada)$"), "GROUP_B").otherwise("CORE").alias("merchant_zone"),
    )
    .orderBy(F.col("account_id").desc(), "merchant_zone")
)
