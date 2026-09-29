from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("merchant").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("merchant", "status").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("merchant").orderBy("dedup_rank", "id")

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") >= "2023-11-18"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)

enriched = (
    with_latest
    .filter(F.col("opened_date").between("2023-06-01", "2025-07-01"))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "account_type_status",
        "dedup_rank",
        "merchant",
        F.ntile(4).over(w_grp).alias("balance_bucket"),
        F.first("merchant", ignorenulls=True).over(w4).alias("first_known_merchant"),
        F.sum("balance").over(w5).alias("running_balance"),
        F.first("status", ignorenulls=True).over(w_part).alias("first_known_status"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([20, 23, 38])), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("opened_date") > "2025-09-29")
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "account_type_status",
        "merchant",
        "balance_bucket",
        "first_known_merchant",
        "running_balance",
        "first_known_status",
        "row_seq",
        F.floor("balance").alias("balance_whole"),
    )
)

base = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-06-01").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.floor("row_seq").alias("row_seq_whole"),
        F.abs("running_balance").alias("abs_running_balance"),
    )
)

result = (
    base
    .rollup(
        F.when(F.col("balance") >= 68000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.max("balance_bucket").alias("max_balance_bucket"),
    )
    .filter(F.col("max_balance_bucket") < 1000000)
)
