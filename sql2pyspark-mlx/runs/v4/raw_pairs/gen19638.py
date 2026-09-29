from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_first())
w4 = Window.partitionBy("account_type").orderBy("balance", "id")
w5 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("next_balance_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("next_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_type").orderBy("cumulative_balance", "id")
w9 = Window.orderBy("running_next_balance_2")
w10 = Window.partitionBy("customer_id").orderBy(F.col("last_known_next_balance_2").desc_nulls_first())
w11 = Window.orderBy(F.col("balance").desc())
w12 = Window.partitionBy("account_type")
w13 = Window.partitionBy("status", "account_type")

scored = (
    df_accounts
    .select(
        "*",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.rank().over(w3).alias("balance_rank"),
        F.lead("balance", 1).over(w4).alias("next_balance_2"),
        F.last("balance", ignorenulls=True).over(w5).alias("last_known_balance"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.last("next_balance_2", ignorenulls=True).over(w6).alias("last_known_next_balance_2"),
        F.sum("next_balance_2").over(w7).alias("running_next_balance_2"),
        F.lead("next_balance", 1).over(w8).alias("next_next_balance"),
    )
)

matched_keys = (
    metrics
    .select("account_type")
    .intersect(
        df_accounts
        .filter(F.col("account_type") != "LOAN")
        .select("account_type")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w9).alias("running_next_balance_2_dense_rank"),
        F.dense_rank().over(w10).alias("last_known_next_balance_2_dense_rank"),
        F.percent_rank().over(w11).alias("balance_percent_rank"),
        F.avg("next_balance").over(w12).alias("account_type_avg_next_balance"),
        F.avg("balance_rank").over(w13).alias("status_avg_balance_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        (F.col("next_next_balance") * F.col("last_known_next_balance_2_dense_rank")).alias("next_next_balance_times_last_known_next_balance_2_dense_rank"),
    )
)

daily = (
    filtered
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("last_known_balance")).otherwise(0)).alias("frozen_last_known_balance"),
    )
    .filter(F.col("distinct_status") >= 1)
)

result = (
    daily
    .select(
        "account_type",
        "cnt",
        "distinct_status",
        F.when(F.col("account_type").rlike("^(LOAN|SAVINGS)$"), "PRIORITY").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "CORE").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy("distinct_status")
)
