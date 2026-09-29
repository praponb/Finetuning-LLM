from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type", "account_id").orderBy("balance_bucket", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy("account_id").orderBy("balance_bucket", "id").rowsBetween(-6, -1)

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-04-19 07:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.balance") <= 30000)
    .select(
        "src.id",
        "src.account_id",
        "src.resolved_merchant",
        F.col("b.balance"),
        F.col("b.account_type"),
    )
)

scored = (
    enriched
    .withColumn("next_balance", F.lead("balance", 2, 0).over(w))
    .withColumn("balance_bucket", F.ntile(4).over(w2))
    .withColumn("running_balance", F.sum("balance").over(w3))
)

scored_transactions = (
    scored
    .select(
        "id",
        "resolved_merchant",
        "running_balance",
        F.avg("next_balance").over(w4).alias("next_balance_avg_5_prior"),
        F.avg("next_balance").over(w_grp).alias("next_balance_avg_6_prior"),
    )
)

result = (
    scored_transactions
    .groupBy(
        F.when(F.col("running_balance") >= 382403, "LARGE").otherwise("SMALL").alias("running_balance_tier"),
    )
    .agg(
        F.percentile_approx("running_balance", [0.25, 0.5, 0.75]).alias("running_balance_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_balance_avg_5_prior", [0.25, 0.5, 0.75]).alias("next_balance_avg_5_prior_quartiles"),
        F.round(F.avg("next_balance_avg_6_prior"), 2).alias("avg_next_balance_avg_6_prior"),
    )
    .orderBy(F.col("running_balance_tier").desc_nulls_last())
)
