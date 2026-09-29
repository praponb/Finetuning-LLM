from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("balance", "id")
w4 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-5, -1)
w6 = Window.orderBy(F.col("credit_limit").desc_nulls_last())
w7 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

scored = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .withColumn("balance_rank", F.rank().over(w))
    .withColumn("balance_bucket", F.ntile(5).over(w2))
    .withColumn("prev_balance", F.lag("balance", 2).over(w3))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w4))
    .withColumn("balance_avg_5_prior", F.avg("balance").over(w5))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant") == "Shell"), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([31, 39])), "left_anti")
)

joined = (
    kept
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.opened_date",
        "src.balance_rank",
        "src.prev_balance",
        "src.last_known_status",
        "src.balance_avg_5_prior",
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
)

matched_keys = (
    joined
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("balance") > 59000)
        .select("id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.credit_limit"), F.col("p.country"))
)

metrics = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w6).alias("credit_limit_cume_dist"),
        F.percent_rank().over(w7).alias("balance_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "balance",
        "balance_rank",
        "balance_avg_5_prior",
        "segment",
        "country",
        "balance_percent_rank",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
