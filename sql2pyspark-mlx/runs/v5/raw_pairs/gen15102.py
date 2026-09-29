from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("is_credit", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("is_credit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 36), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") >= F.lit("2023-04-13").cast("date")))
    .select(
        "*",
        F.when(F.col("account_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

metrics = (
    filtered
    .filter(F.col("balance").between(35000, 40000))
    .select(
        "id",
        "account_type",
        "status",
        "is_credit",
        "status_filled",
        F.first("status", ignorenulls=True).over(w).alias("first_known_status"),
        F.avg("is_credit").over(w2).alias("is_credit_avg_5"),
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status_2"),
        F.sum("is_credit").over(w4).alias("cumulative_is_credit"),
        F.first("balance", ignorenulls=True).over(w5).alias("first_known_balance"),
    )
)

not_flagged_accounts = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") >= 2000), "left_anti")
)

result = (
    not_flagged_accounts
    .cube(
        F.when(F.col("cumulative_is_credit") >= 311, "LARGE").otherwise("SMALL").alias("cumulative_is_credit_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.count("*").alias("event_count"),
        F.round(F.avg("first_known_balance"), 2).alias("avg_first_known_balance"),
    )
    .filter(F.col("open_rows") < 1000)
    .orderBy(F.col("avg_first_known_balance").asc_nulls_last())
)
