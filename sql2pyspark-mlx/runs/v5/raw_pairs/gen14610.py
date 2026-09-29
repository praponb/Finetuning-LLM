from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type", "status").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("country", "customer_id").orderBy("opened_date", "id")
w6 = Window.partitionBy("customer_id").orderBy(F.col("row_seq").desc_nulls_last(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.avg("balance").over(w).alias("balance_avg_5_prior"),
        F.ntile(4).over(w2).alias("balance_bucket"),
        F.ntile(4).over(w3).alias("balance_bucket_2"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.opened_date",
        "src.status",
        "src.balance_bucket",
        "src.row_seq",
        F.col("d.name"),
        F.col("d.country"),
    )
)

scored = (
    with_dims
    .select(
        "*",
        F.lead("balance_bucket").over(w5).alias("next_balance_bucket"),
        F.lead("row_seq").over(w5).alias("next_row_seq"),
        F.lag("row_seq").over(w5).alias("prev_row_seq"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.txn_type").isin(["DEBIT", "FEE"])), "left_anti")
)

deduped = (
    kept
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

result = (
    with_latest
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .groupingSets(
        [["week_start", "year_start"], ["week_start"], []],
        "week_start",
        "year_start",
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("transaction_amount", [0.25, 0.5, 0.75]).alias("transaction_amount_quartiles"),
    )
)
