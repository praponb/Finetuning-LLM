from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w_part = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-5, -1)
w_recent = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("seq_no").desc_nulls_last())

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .withColumn("next_balance", F.lead("balance", 1).over(w))
    .withColumn("balance_avg_5_prior", F.avg("balance").over(w_part))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.country"), F.col("d.email"), F.col("d.city"))
)

with_windows = (
    joined
    .withColumn("first_known_next_balance", F.first("next_balance", ignorenulls=True).over(w4))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "next_balance",
        "balance_avg_5_prior",
        "seq_no",
    )
)
