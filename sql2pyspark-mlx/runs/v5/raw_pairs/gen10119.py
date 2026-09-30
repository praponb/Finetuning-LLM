from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_account_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-10-24 14:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.lower("txn_type").alias("txn_type_lower"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.balance") > 35000)
    .select(
        "a.id",
        "a.account_id",
        "a.txn_time",
        "a.merchant",
        "a.is_flagged",
        "a.txn_type_lower",
        F.col("b.balance"),
        F.col("b.status"),
        F.col("b.account_type"),
    )
)

valid = (
    joined
    .filter(F.col("txn_time") >= F.to_date(F.lit("2023-10-15")))
    .withColumn("merchant_lower", F.lower("merchant"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_time",
        "s.txn_type_lower",
        "s.account_type",
        "s.merchant_lower",
        F.col("a.account_type").alias("account_account_type"),
    )
)

with_gaps = (
    with_dims
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_time",
        "txn_type_lower",
        "merchant_lower",
        "account_account_type",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .select(
        "id",
        "merchant_lower",
        "account_account_type",
        F.when(F.col("account_account_type").rlike("^(CREDIT|LOAN)$"), "GROUP_A").otherwise("GROUP_B").alias("account_account_type_zone"),
    )
)
