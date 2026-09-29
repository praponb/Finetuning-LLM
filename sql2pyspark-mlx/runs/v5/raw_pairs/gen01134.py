from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-6, -1)
w_recent = Window.orderBy(F.col("transaction_txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_recent_2 = Window.partitionBy("customer_id")

filtered = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("FROZEN"))) & (F.col("opened_date") >= F.lit("2023-05-26").cast("date")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
        F.lower("status").alias("status_lower"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

scored = (
    with_last
    .select(
        "id",
        "balance",
        "opened_date",
        "status_lower",
        F.avg("is_frozen").over(w2).alias("is_frozen_avg_6_prior"),
        F.count("is_frozen").over(w_recent).alias("is_frozen_count_90d"),
        F.avg("balance").over(w_recent_2).alias("customer_id_avg_balance"),
    )
)

cleaned = (
    scored
    .filter(F.col("opened_date") > F.lit("2023-08-10").cast("date"))
    .select(
        "id",
        "opened_date",
        "status_lower",
        "is_frozen_avg_6_prior",
        "is_frozen_count_90d",
        "customer_id_avg_balance",
        F.coalesce("customer_id_avg_balance", F.lit(0)).alias("customer_id_avg_balance_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "opened_date",
        "status_lower",
        "is_frozen_avg_6_prior",
        "is_frozen_count_90d",
        "customer_id_avg_balance",
        "customer_id_avg_balance_filled",
    )
    .orderBy(F.col("customer_id_avg_balance_filled").desc_nulls_first())
)
