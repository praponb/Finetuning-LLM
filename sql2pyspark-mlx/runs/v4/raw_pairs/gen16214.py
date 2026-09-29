from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 35000) & ((F.col("status") != "CLOSED") | F.col("customer_id").isin([24, 33])))
    .groupBy("account_type", "customer_id")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
