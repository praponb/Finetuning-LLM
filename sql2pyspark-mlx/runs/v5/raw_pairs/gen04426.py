from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance_int").desc())
w_part = Window.partitionBy("account_type", "status").orderBy("balance", "id")

cleaned = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.col("balance").cast("int").alias("balance_int"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .withColumn("next_balance_int", F.lead("balance_int").over(w_part))
    .withColumn("next_balance", F.lead("balance").over(w_part))
    .withColumn("prev_balance", F.lag("balance").over(w_part))
    .withColumn("event_seq", F.row_number().over(w_part))
)

not_flagged_accounts = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "REFUND"), "left_anti")
)

result = (
    not_flagged_accounts
    .groupBy("customer_id")
    .pivot("status", ["OPEN", "CLOSED", "FROZEN"])
    .agg(F.max("balance"))
)
