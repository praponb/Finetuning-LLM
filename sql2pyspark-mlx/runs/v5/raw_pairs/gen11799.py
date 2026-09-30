from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("first_known_balance", "id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-12"))) & F.col("opened_date").between("2024-05-01", "2025-03-01"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

metrics = (
    base
    .select(
        "id",
        "account_type",
        "status",
        "status_prefix",
        F.sum("balance").over(w_grp).alias("running_balance"),
        F.ntile(4).over(w_part).alias("balance_bucket"),
        F.first("balance", ignorenulls=True).over(w_grp).alias("first_known_balance"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < "2025-04-08"), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "account_type",
        "status",
        "running_balance",
        "balance_bucket",
        "first_known_balance",
        F.when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "PRIORITY").when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "GROUP_B").otherwise("GROUP_A").alias("account_type_zone"),
    )
)
