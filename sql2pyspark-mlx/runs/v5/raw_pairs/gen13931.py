from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .rollup("year_start", "customer_id", "quarter_start")
    .agg(
        F.max(F.when(F.col("status") == "CLOSED", F.col("balance"))).alias("closed_max_balance"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_per_checking"),
        F.count("*").alias("row_count"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy("year_start")
)
