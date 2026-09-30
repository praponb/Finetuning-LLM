from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")
w_recent = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w7 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("account_id").isin([4, 21]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.when(F.col("amount") >= 3900, "High").when(F.col("amount") >= 900, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.account_type"), F.col("a.opened_date"))
)

metrics = (
    with_dims
    .filter(F.month("opened_date").isin([1, 8, 10]))
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "amount_level",
        "opened_date",
        F.last("account_type", ignorenulls=True).over(w).alias("last_known_account_type"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("account_type", ignorenulls=True).over(w_recent).alias("first_known_account_type"),
        F.first("account_type", ignorenulls=True).over(w5).alias("first_known_account_type_2"),
    )
)

cleaned = (
    metrics
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-09-16 08:00:00")))
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "amount_level",
        "last_known_account_type",
        "row_seq",
        "seq_no",
        "first_known_account_type",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.month("txn_time").alias("txn_time_month"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "amount_level",
        "row_seq",
        "seq_no",
        F.sum("session_start_flag").over(w7).alias("island_id"),
    )
)

with_gaps = (
    sessionized
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id_2", F.sum("session_start_flag").over(w7))

result = (
    islands
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .groupBy(F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.island_id")), 2).alias("avg_island_id"),
    )
)
