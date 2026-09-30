from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("txn_type").orderBy("last3_min_balance", "id")
w_hist_2 = Window.partitionBy("customer_id")

enriched = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.status",
        F.col("d.email"),
        F.col("d.city"),
        F.col("d.credit_limit"),
    )
)

scored = (
    enriched
    .filter(F.col("email").like("%@company.co"))
    .select(
        "*",
        F.min("balance").over(w).alias("last3_min_balance"),
        F.max("balance").over(w).alias("last3_max_balance"),
        F.count("balance").over(w).alias("last3_count_balance"),
        F.first("balance", ignorenulls=True).over(w2).alias("first_known_balance"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"), F.col("c.merchant"))
)

scored_accounts = (
    with_latest
    .withColumn("next_last3_max_balance", F.lead("last3_max_balance").over(w_hist))
    .withColumn("prev_last3_max_balance", F.lag("last3_max_balance").over(w_hist))
    .withColumn("prev_balance", F.lag("balance").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("customer_id_max_first_known_balance", F.max("first_known_balance").over(w_hist_2))
)

result = (
    scored_accounts
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.segment").alias("segment"),
        F.col("s.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.seq_no")), 2).alias("avg_seq_no"),
        F.round(F.avg(F.col("s.last3_min_balance")), 2).alias("avg_last3_min_balance"),
    )
    .orderBy("txn_type")
)
