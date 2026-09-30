from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("account_id").orderBy("amount", "id")
w_hist = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first())
w3 = Window.partitionBy("merchant").orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "account_id").orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id", "merchant")
w7 = Window.partitionBy("merchant", "status").orderBy("account_id_max_amount_rank")
w8 = Window.partitionBy("account_id")
w9 = Window.partitionBy("account_id", "txn_type").orderBy("first_known_amount", "id")

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.ntile(4).over(w_grp).alias("amount_bucket"),
        F.rank().over(w_hist).alias("amount_rank"),
    )
)

enriched = (
    metrics
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.status"))
)

scored = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "amount_bucket",
        "amount_rank",
        "status",
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
        F.sum("amount").over(w5).alias("cumulative_amount_2"),
        F.max("amount_rank").over(w6).alias("account_id_max_amount_rank"),
    )
)

metrics_transactions = (
    scored
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "amount_bucket",
        "status",
        "cumulative_amount_2",
        "account_id_max_amount_rank",
        F.cume_dist().over(w7).alias("account_id_max_amount_rank_cume_dist"),
        F.round((F.col("amount_rank") / F.sum("amount_rank").over(w8)), 4).alias("amount_rank_share"),
        F.lead("amount_bucket", 2).over(w9).alias("next_amount_bucket"),
    )
)

result = (
    metrics_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "status",
        "cumulative_amount_2",
        "amount_rank_share",
        "next_amount_bucket",
        F.when(F.col("txn_type").rlike("^(FEE|CREDIT)$"), "CORE").when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("txn_type_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last(), "next_amount_bucket")
)
