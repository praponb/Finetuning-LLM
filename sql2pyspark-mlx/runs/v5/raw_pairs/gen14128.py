from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("adjusted_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("adjusted_balance").asc_nulls_first())

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-05-06"))) & F.col("status").isNotNull())
    .withColumn("adjusted_balance", F.round((F.col("balance") * 1.07), 2))
)

eligible = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= "2025-09-01"), "left_semi")
)

valid = (
    eligible
    .filter(F.col("opened_date") >= F.lit("2023-03-05").cast("date"))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "adjusted_balance",
        F.when(F.col("balance") >= 59000, "High").when(F.col("balance") >= 6000, "Medium").otherwise("Low").alias("balance_band"),
        F.round((F.col("adjusted_balance") * 100), 2).alias("adjusted_balance_adjusted"),
    )
)

enriched = (
    valid
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "adjusted_balance",
        "adjusted_balance_adjusted",
        F.sum("balance").over(w).alias("running_balance"),
        F.sum("adjusted_balance").over(w2).alias("running_adjusted_balance"),
        F.first("adjusted_balance", ignorenulls=True).over(w3).alias("first_known_adjusted_balance"),
        F.sum("adjusted_balance_adjusted").over(w4).alias("cumulative_adjusted_balance_adjusted"),
        F.rank().over(w5).alias("adjusted_balance_rank"),
    )
)

base_accounts = (
    enriched
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-09-08"))) & (F.col("first_known_adjusted_balance") <= 25))
    .withColumn("least_adjusted_balance_adjusted_balance", F.least("adjusted_balance_adjusted", "balance"))
)

valid_accounts = (
    base_accounts
    .filter(F.col("adjusted_balance") < 40)
    .select(
        "id",
        "balance",
        "opened_date",
        "adjusted_balance",
        "adjusted_balance_adjusted",
        "running_balance",
        "running_adjusted_balance",
        "cumulative_adjusted_balance_adjusted",
        "adjusted_balance_rank",
        "least_adjusted_balance_adjusted_balance",
        (F.col("adjusted_balance") + F.col("adjusted_balance_adjusted")).alias("adjusted_balance_plus_adjusted_balance_adjusted"),
    )
)

not_flagged = (
    valid_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "opened_date",
        "adjusted_balance",
        "adjusted_balance_adjusted",
        "running_balance",
        "cumulative_adjusted_balance_adjusted",
        "least_adjusted_balance_adjusted_balance",
        "adjusted_balance_plus_adjusted_balance_adjusted",
    )
    .orderBy("opened_date")
)
