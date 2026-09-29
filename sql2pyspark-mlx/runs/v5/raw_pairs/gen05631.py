from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy(F.col("balance_adjusted").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("balance_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last_known_balance_adjusted", "id")
w5 = Window.partitionBy("customer_id").orderBy("cumulative_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status")

valid = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & F.col("customer_id").isNotNull())
    .withColumn("balance_adjusted", F.round((F.col("balance") * 100), 2))
    .withColumn("status_filled", F.coalesce("status", F.lit("unassigned")))
)

with_windows = (
    valid
    .withColumn("last_known_balance_adjusted", F.last("balance_adjusted", ignorenulls=True).over(w_running))
    .withColumn("balance_adjusted_dense_rank", F.dense_rank().over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
)

cleaned = (
    with_windows
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-08-19")))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "status_filled",
        "last_known_balance_adjusted",
        "cumulative_balance",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

scored = (
    cleaned
    .filter(F.col("cumulative_balance") >= 181787)
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "cumulative_balance",
        "status_title",
        F.row_number().over(w4).alias("row_seq"),
        F.last("status", ignorenulls=True).over(w5).alias("last_known_status"),
        F.count("cumulative_balance").over(w6).alias("status_count_cumulative_balance"),
    )
)

result = (
    scored
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .rollup(F.col("d.segment").alias("segment"), F.col("d.country").alias("country"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.row_seq")).alias("min_row_seq"),
        F.sum(F.col("e.cumulative_balance")).alias("total_cumulative_balance"),
    )
    .orderBy("total_cumulative_balance")
)
