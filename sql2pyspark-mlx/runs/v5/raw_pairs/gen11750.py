from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, -1)

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-05-13").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.upper("account_type").alias("account_type_upper"),
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)

scored = (
    valid
    .filter(F.month("opened_date").isin([6, 11, 12]))
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_5_prior"),
        F.avg("balance").over(w2).alias("balance_avg_6_prior"),
    )
)

valid_accounts = (
    scored
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "account_type_upper",
        F.lower("account_type").alias("account_type_lower"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

kept = (
    valid_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.country").alias("country"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("e.balance")), 2).alias("total_balance"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
