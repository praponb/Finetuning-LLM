from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("adjusted_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy(F.col("is_open").desc())
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.orderBy("is_open")
w_hist = Window.partitionBy("status").orderBy(F.col("running_balance").desc_nulls_last())
w_recent = Window.partitionBy("customer_id")
w7 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_first())
w_ordered = Window.orderBy(F.col("balance").asc_nulls_last())

base = (
    df_accounts
    .filter(F.col("balance").between(56000, 59000))
    .withColumn("adjusted_balance", F.round((F.col("balance") * 0.9), 2))
    .withColumn("is_open", F.when(F.col("status") == "OPEN", 1).otherwise(0))
)

with_windows = (
    base
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("is_open_rank", F.rank().over(w2))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

kept_keys = (
    latest
    .select("status", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
        .select("status", "customer_id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("is_open_cume_dist", F.cume_dist().over(w4))
    .withColumn("running_balance_rank", F.rank().over(w_hist))
    .withColumn("customer_id_count_running_balance", F.count("running_balance").over(w_recent))
    .withColumn("balance_cume_dist", F.cume_dist().over(w7))
    .withColumn("balance_dense_rank", F.dense_rank().over(w_ordered))
)

result = (
    enriched
    .select(
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "running_balance",
        "is_open_rank",
        "rn",
        "running_balance_rank",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "GROUP_A").when(F.col("status").rlike("^(OPEN)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("is_open_rank").desc_nulls_first())
)
