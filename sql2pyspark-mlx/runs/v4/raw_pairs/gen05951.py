from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.orderBy("balance_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc_nulls_first())
w4 = Window.partitionBy("account_type", "status").orderBy("balance_bucket", "id")
w5 = Window.partitionBy("status").orderBy("rn", "id")
w6 = Window.partitionBy("account_type", "status").orderBy(F.col("balance_rounded").desc_nulls_last())
w7 = Window.partitionBy("status")

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isin([32, 39]))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 0).alias("balance_rounded"),
    )
)

enriched = (
    cleaned
    .withColumn("balance_bucket", F.ntile(4).over(w))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w2))
    .withColumn("balance_rank", F.rank().over(w3))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.lead("balance_rounded", 1).over(w5).alias("next_balance_rounded"),
        F.cume_dist().over(w6).alias("balance_rounded_cume_dist"),
        F.round((F.col("balance_rank") / F.sum("balance_rank").over(w7)), 4).alias("balance_rank_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "balance_rounded",
        "balance_bucket",
        "last_known_status",
        "rn",
        "next_balance_rounded",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "OTHER_ZONE").when(F.col("status").rlike("^(OPEN)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("rn").desc_nulls_first(), "next_balance_rounded")
)
