from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("balance_filled", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("status").orderBy("balance_filled", "id")
w3 = Window.partitionBy("status").orderBy("is_closed", "id")
w4 = Window.partitionBy("status").orderBy(F.col("next_balance_filled").desc_nulls_last(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("opened_date").desc_nulls_first(), "id")

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("balance").between(44000, 64000))
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

valid = (
    cleaned
    .filter((F.col("opened_date") >= F.lit("2023-10-14").cast("date")) & F.col("status").isNotNull() & (F.col("balance_filled") >= 45))
    .select(
        "id",
        "opened_date",
        "status",
        "status_title",
        "balance_filled",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (~F.col("c.is_flagged")), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.avg("is_closed").over(w_part).alias("is_closed_avg_5_prior"),
        F.ntile(10).over(w_running).alias("balance_filled_bucket"),
        F.lead("balance_filled", 1).over(w3).alias("next_balance_filled"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select(
        "id",
        "opened_date",
        "status_title",
        "balance_filled",
        "opened_date_month_end",
        "is_closed",
        "is_closed_avg_5_prior",
        "balance_filled_bucket",
        "next_balance_filled",
    )
    .orderBy(F.col("balance_filled_bucket").desc_nulls_last())
)
