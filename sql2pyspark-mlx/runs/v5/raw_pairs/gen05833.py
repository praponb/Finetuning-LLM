from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("event_seq", "id")
w4 = Window.partitionBy("account_type", "status").orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.orderBy(F.col("event_seq").desc(), "id")

enriched = (
    df_accounts
    .filter(F.col("customer_id") == 31)
    .select(
        "*",
        F.lag("balance").over(w_grp).alias("prev_balance"),
        F.lead("balance").over(w_grp).alias("next_balance"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lead("balance").over(w2).alias("next_balance_2"),
        F.lag("next_balance").over(w2).alias("prev_next_balance"),
        F.row_number().over(w2).alias("seq_no"),
        F.lag("balance", 1).over(w3).alias("prev_balance_2"),
    )
)

base = (
    scored
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]) & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["SAVINGS", "LOAN"]), "Tier 1").otherwise("Other"))
)

scored_accounts = (
    base
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "next_balance",
        "next_balance_2",
        "seq_no",
        "account_type_group",
        F.min("prev_balance_2").over(w4).alias("last7_min_prev_balance_2"),
        F.avg("prev_balance_2").over(w4).alias("last7_avg_prev_balance_2"),
        F.sum("prev_balance_2").over(w4).alias("last7_sum_prev_balance_2"),
        F.ntile(3).over(w_part).alias("event_seq_bucket"),
    )
)

result = (
    scored_accounts
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .cube("customer_id", "week_start")
    .agg(
        F.round(F.sum("next_balance") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("next_balance_per_checking"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("event_seq_bucket")).otherwise(0)).alias("savings_event_seq_bucket"),
        F.round(F.avg("next_balance_2"), 2).alias("avg_next_balance_2"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("seq_no"))).alias("checking_total_seq_no"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("customer_id").desc())
)
