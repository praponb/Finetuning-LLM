from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-30").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.upper("status").alias("status_upper"),
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)

enriched = (
    valid
    .filter(F.col("balance") > 68000)
    .select(
        "*",
        F.lag("balance").over(w_recent).alias("prev_balance"),
        F.lead("balance").over(w_recent).alias("next_balance"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("balance").over(w2).alias("balance_avg_3_prior"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status_upper",
        "s.account_type_title",
        "s.next_balance",
        "s.seq_no",
        F.col("c.merchant"),
    )
)

kept_keys = (
    with_last
    .select("id")
    .intersectAll(
        df_accounts
        .filter(F.col("status") != "OPEN")
        .select("id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("seq_no", "merchant")
    .agg(
        F.percentile_approx("next_balance", [0.25, 0.5, 0.75]).alias("next_balance_quartiles"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") < 1000)
)
