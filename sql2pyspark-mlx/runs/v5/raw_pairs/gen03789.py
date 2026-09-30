from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-06-27").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "account_type_title",
    )
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

latest_match = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"), F.col("c.txn_time"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.txn_time")), "left_anti")
)

result = (
    eligible
    .groupBy("customer_id")
    .pivot("txn_type", ["DEBIT", "CREDIT"])
    .agg(F.sum("balance").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
