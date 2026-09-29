from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id").orderBy("next_balance", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("balance") > 36000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.upper("account_type").alias("account_type_upper"),
    )
)

enriched = (
    base
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.lag("balance").over(w).alias("prev_balance"),
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("row_seq"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

with_windows = (
    with_latest
    .withColumn("last7_min_next_balance", F.min("next_balance").over(w3))
    .withColumn("last7_max_next_balance", F.max("next_balance").over(w3))
    .withColumn("last7_avg_next_balance", F.avg("next_balance").over(w3))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.min("balance").alias("min_balance"),
        F.percentile_approx("prev_balance", 0.9).alias("prev_balance_p90"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.percentile_approx("next_balance", [0.25, 0.5, 0.75]).alias("next_balance_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
