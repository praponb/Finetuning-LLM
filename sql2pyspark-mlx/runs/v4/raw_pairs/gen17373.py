from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("account_type", "status")

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_semi")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.rank().over(w).alias("balance_rank"),
        F.avg("balance").over(w2).alias("balance_avg_2_prior"),
        F.max("balance").over(w3).alias("account_type_max_balance"),
    )
)

kept_keys = (
    metrics
    .select("account_type", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "OPEN"]))
        .select("account_type", "customer_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.credit_limit"))
)

kept_keys_2 = (
    joined
    .select("id")
    .subtract(
        df_accounts
        .filter(F.month("opened_date").isin([4, 6, 9]))
        .select("id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    reconciled
    .groupBy("account_type")
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.round(F.avg("balance_avg_2_prior"), 2).alias("avg_balance_avg_2_prior"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
)

result = (
    daily
    .select(
        "account_type",
        "avg_balance_avg_2_prior",
        "num_rows",
        "open_count",
        "savings_count",
        F.when(F.col("account_type").rlike("^(CHECKING|SAVINGS)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "EXTENDED").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy(F.col("account_type_zone").desc_nulls_last())
)
