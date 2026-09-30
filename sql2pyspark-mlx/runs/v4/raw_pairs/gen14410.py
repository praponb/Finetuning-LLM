from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.last("status", ignorenulls=True).over(w).alias("last_known_status"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.cume_dist().over(w3).alias("balance_cume_dist"),
    )
)

remaining_keys = (
    enriched
    .select("id", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("opened_date") >= "2024-08-03")
        .select("id", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched_accounts = (
    verified
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.name"))
)

result = (
    enriched_accounts
    .withColumn("balance_tier", F.when(F.col("balance") >= 79000, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .groupingSets(
        [["balance_tier", "week_start"], ["balance_tier"], []],
        "balance_tier",
        "week_start",
    )
    .agg(
        F.round(F.sum("running_balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("running_balance_per_closed"),
        F.avg(F.when(F.col("status") == "OPEN", F.col("balance"))).alias("open_avg_balance"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("open_avg_balance") > 0)
)
