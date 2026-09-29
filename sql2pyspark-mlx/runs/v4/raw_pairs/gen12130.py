from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time_day", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time_day", "id")
w5 = Window.orderBy("next_amount", "id")
w6 = Window.partitionBy("txn_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("amount_avg_6_prior").desc())

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-12").cast("date")) & F.col("merchant").isNotNull() & (F.col("amount") >= 1300))
    .select(
        "*",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_6_prior"),
        F.max("amount").over(w2).alias("amount_max_30d"),
        F.lead("amount", 2).over(w3).alias("next_amount"),
    )
)

joined = (
    metrics
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select("a.*", F.col("p2.opened_date"))
)

with_windows = (
    joined
    .withColumn("next_amount_2", F.lead("amount", 2).over(w4))
    .withColumn("next_amount_bucket", F.ntile(4).over(w5))
    .withColumn("cumulative_amount", F.sum("amount").over(w6))
    .withColumn("amount_avg_6_prior_dense_rank", F.dense_rank().over(w7))
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("MONTH", "txn_time_day").alias("activity_month"),
        "account_id",
    )
    .agg(
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.percentile_approx("amount_avg_6_prior", [0.25, 0.5, 0.75]).alias("amount_avg_6_prior_quartiles"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.percentile_approx("amount_avg_6_prior", 0.95).alias("amount_avg_6_prior_p95"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("distinct_txn_types") < 1)
    .orderBy(F.col("distinct_txn_types").asc_nulls_first())
)
