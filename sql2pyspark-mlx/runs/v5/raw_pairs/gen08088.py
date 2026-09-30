from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("last4_count_amount").desc(), "id")
w3 = Window.orderBy("last4_min_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy(F.col("last4_max_amount").desc(), "id")

enriched = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("last4_max_amount", F.max("amount").over(w))
    .withColumn("last4_count_amount", F.count("amount").over(w))
    .withColumn("last4_min_amount", F.min("amount").over(w))
)

scored = (
    enriched
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "last4_max_amount",
        "last4_count_amount",
        "last4_min_amount",
        F.row_number().over(w2).alias("row_seq"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.sum("last4_count_amount").over(w4).alias("cumulative_last4_count_amount"),
        F.sum("last4_max_amount").over(w5).alias("cumulative_last4_max_amount"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

kept_keys = (
    scored
    .select("account_id", "id")
    .subtract(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("account_id", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.opened_date"))
)

joined_transactions = (
    joined
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .select(
        "src.account_id",
        "src.last4_count_amount",
        "src.row_seq",
        "src.cumulative_last4_count_amount",
        "src.cumulative_last4_max_amount",
        "src.seq_no",
        "src.opened_date",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.account_type"),
        F.col("b.balance"),
    )
)

with_dims = (
    joined_transactions
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.balance").alias("account_balance"),
        F.col("p.account_type").alias("account_account_type"),
        F.col("p.status"),
    )
)

result = (
    with_dims
    .select(
        "last4_count_amount",
        "row_seq",
        "cumulative_last4_max_amount",
        "seq_no",
        "account_opened_date",
        "account_type",
        "balance",
        "account_account_type",
        "status",
        F.when(F.col("account_account_type").rlike("^(LOAN|SAVINGS)$"), "GROUP_A").when(F.col("account_account_type").rlike("^(CHECKING|CREDIT)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("account_account_type_zone"),
    )
    .orderBy(F.col("cumulative_last4_max_amount").desc_nulls_last())
)
