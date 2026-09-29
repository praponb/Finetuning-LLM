from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("txn_time", "id")
w4 = Window.orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("account_id", "merchant").orderBy(F.col("days_since_opened_date").desc_nulls_last())
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "merchant",
        F.count("amount").over(w).alias("txn_type_count_amount"),
        F.last("merchant", ignorenulls=True).over(w2).alias("last_known_merchant"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
        F.percent_rank().over(w4).alias("amount_percent_rank"),
    )
)

joined = (
    metrics
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.account_type") != "CHECKING")
    .select("a.*", F.col("b.opened_date"))
)

enriched = (
    joined
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.account_type"), F.col("d.balance"), F.col("d.status"))
)

cleaned = (
    enriched
    .filter(F.col("prev_amount").isNotNull())
    .select(
        "id",
        "account_id",
        "merchant",
        "txn_type_count_amount",
        "last_known_merchant",
        "prev_amount",
        "amount_percent_rank",
        "opened_date",
        "account_type",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("account_type", F.lit("DIRECT")).alias("account_type_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.rank().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "merchant",
        "txn_type_count_amount",
        "amount_percent_rank",
        "opened_date",
        "account_type",
        "days_since_opened_date",
        "account_type_filled",
        F.when(F.col("account_type").rlike("^(CREDIT|SAVINGS)$"), "GROUP_A").when(F.col("account_type").rlike("^(CHECKING|LOAN)$"), "EXTENDED").otherwise("GROUP_B").alias("account_type_zone"),
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("sum_transaction_amount").asc_nulls_last(), "id")
)
