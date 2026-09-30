from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .groupingSets(
        [["year_start", "quarter_start", "customer_id"], ["year_start", "quarter_start"]],
        "year_start",
        "quarter_start",
        "customer_id",
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.avg(F.when(F.col("account_type") == "SAVINGS", F.col("balance"))).alias("savings_avg_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
