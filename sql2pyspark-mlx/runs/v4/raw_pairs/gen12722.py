from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("merchant").orderBy("amount", "id")
w5 = Window.partitionBy("merchant").orderBy("account_opened_date")
w6 = Window.partitionBy("status").orderBy("txn_time", "id")
w7 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-02 19:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
    )
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
        F.avg("amount").over(w3).alias("amount_avg_2_prior"),
        F.ntile(10).over(w4).alias("amount_bucket"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.balance"), F.col("b.status"), F.col("b.opened_date"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.opened_date").alias("account_opened_date"))
)

ranked = (
    joined
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w7))

with_dims_transactions = (
    islands
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.*",
        F.col("a.status").alias("account_status"),
        F.col("a.balance").alias("account_balance"),
    )
)

daily = (
    with_dims_transactions
    .cube(
        F.when(F.col("amount_avg_2_prior") >= 3822, "LARGE").otherwise("SMALL").alias("amount_avg_2_prior_tier"),
        F.date_trunc("YEAR", "account_opened_date").alias("year_start"),
        F.when(F.col("burst_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.percentile_approx("amount_bucket", 0.95).alias("amount_bucket_p95"),
        F.avg(F.when(F.col("merchant") == "7-Eleven", F.col("burst_id"))).alias("7_eleven_avg_burst_id"),
    )
)

result = daily.select("year_start")
