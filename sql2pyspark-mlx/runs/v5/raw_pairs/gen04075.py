from pyspark.sql import functions as F

result = (
    df_accounts
    .cube(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
)
