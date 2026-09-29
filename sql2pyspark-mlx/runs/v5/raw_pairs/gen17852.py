from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").desc(), "id")

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-10-16 01:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "account_id", "merchant", "is_flagged", "merchant_lower")
)

joined = (
    ranked
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.balance").isNotNull())
    .select("e.*", F.col("b.status"), F.col("b.opened_date"), F.col("b.account_type"))
)

cleaned = (
    joined
    .filter(F.col("merchant").isNotNull() & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "*",
        F.concat_ws("/", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("resolved_account_type"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "merchant_lower",
        "opened_date",
        "account_type_status_key",
        "resolved_account_type",
    )
    .orderBy(F.col("opened_date").desc())
)
