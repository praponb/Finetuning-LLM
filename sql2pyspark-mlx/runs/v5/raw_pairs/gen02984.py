from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isin([22, 32]) & (F.col("account_type") != "LOAN")) | (F.col("opened_date") >= "2023-06-17"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("avg_balance").desc(), "stddev_balance")
)
