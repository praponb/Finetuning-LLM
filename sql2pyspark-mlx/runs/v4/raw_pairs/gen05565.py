from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "status")

deduped = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

valid = (
    deduped
    .filter((F.col("txn_time") >= F.lit("2024-05-30 03:00:00").cast("timestamp")) & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "row_num",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

valid_transactions = (
    valid
    .filter(F.col("txn_time") >= F.lit("2024-02-18 14:00:00").cast("timestamp"))
    .withColumn("is_flagged_flag_2", F.col("is_flagged").cast("int"))
)

filtered = (
    valid_transactions
    .filter(F.col("is_flagged_flag_2").between(57, 62))
    .select(
        "*",
        F.round((F.col("is_flagged_flag_2") / F.col("row_num")), 2).alias("is_flagged_flag_2_per_row_num"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag_3"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.status"))
)

result = (
    joined
    .select(
        "*",
        F.first("is_flagged_flag_3", ignorenulls=True).over(w2).alias("first_known_is_flagged_flag_3"),
        F.sum("is_flagged_flag_2").over(w3).alias("running_is_flagged_flag_2"),
        F.avg("is_flagged_flag").over(w4).alias("merchant_avg_is_flagged_flag"),
    )
)
