from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy(F.col("age").desc_nulls_first())
w2 = Window.partitionBy("account_type")
w_grp = Window.partitionBy("account_type", "status")
w_running = Window.orderBy("age")

joined = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.age"), F.col("d.segment"))
)

matched_keys = (
    joined
    .select("id", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
        .select("id", "customer_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

not_flagged_accounts = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_semi")
)

metrics = (
    not_flagged_accounts
    .withColumn("age_rank", F.rank().over(w_ordered))
    .withColumn("account_type_min_balance", F.min("balance").over(w2))
    .withColumn("account_type_max_balance", F.max("balance").over(w_grp))
    .withColumn("age_percent_rank", F.percent_rank().over(w_running))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "segment",
        "age_rank",
        "account_type_min_balance",
        F.when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "GROUP_A").otherwise("EXTENDED").alias("account_type_zone"),
    )
    .orderBy("age_rank", "account_type")
)
