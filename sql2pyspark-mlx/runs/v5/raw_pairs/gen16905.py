from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"], []],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
    )
    .filter(F.col("closed_events") > 1)
    .orderBy(F.col("avg_balance").desc_nulls_first())
)
