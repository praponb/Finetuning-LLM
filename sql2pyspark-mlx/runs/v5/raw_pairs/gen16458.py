from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status", "account_type").orderBy("running_sum_balance", "id")
w3 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy("running_sum_balance", "id")
w5 = Window.partitionBy("account_type").orderBy("next_running_sum_balance")
w6 = Window.partitionBy("account_type").orderBy("running_max_balance", "id")
w7 = Window.partitionBy("account_type").orderBy("running_sum_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("balance") > 67000)
    .select("*", F.initcap(F.trim("account_type")).alias("account_type_title"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "account_type_title",
        F.max("balance").over(w_running).alias("running_max_balance"),
        F.avg("balance").over(w_running).alias("running_avg_balance"),
        F.sum("balance").over(w_running).alias("running_sum_balance"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "account_type",
        "balance",
        "running_max_balance",
        "running_avg_balance",
        "running_sum_balance",
        F.lead("running_sum_balance", 1).over(w_ordered).alias("next_running_sum_balance"),
        F.sum("running_avg_balance").over(w3).alias("cumulative_running_avg_balance"),
    )
)

scored = (
    enriched
    .filter(F.col("cumulative_running_avg_balance").between(242476, 310829))
    .withColumn("prev_running_max_balance", F.lag("running_max_balance", 1, 0).over(w4))
    .withColumn("next_running_sum_balance_percent_rank", F.percent_rank().over(w5))
    .withColumn("prev_running_max_balance_2", F.lag("running_max_balance", 2, 0).over(w6))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w7))
)

filtered = (
    scored
    .filter(F.col("balance").between(56000, 75000))
    .select(
        "id",
        "balance",
        "running_avg_balance",
        "next_running_sum_balance",
        "cumulative_running_avg_balance",
        "prev_running_max_balance",
        "next_running_sum_balance_percent_rank",
        "last_known_balance",
        F.when(F.col("last_known_balance") >= 70000, "High").when(F.col("last_known_balance") >= 29000, "Medium").otherwise("Low").alias("last_known_balance_level"),
    )
)

totals = (
    filtered
    .groupBy(
        F.when(F.col("running_avg_balance") >= 67000, "PREMIUM").otherwise("STANDARD").alias("running_avg_balance_tier"),
        F.when(F.col("cumulative_running_avg_balance") >= 384394, "PREMIUM").otherwise("STANDARD").alias("cumulative_running_avg_balance_tier"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.percentile_approx("last_known_balance", [0.25, 0.5, 0.75]).alias("last_known_balance_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "running_avg_balance_tier",
        "cumulative_running_avg_balance_tier",
        "total_balance",
        "last_known_balance_quartiles",
    )
)
