from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "LOAN"]) & ((F.col("opened_date") < "2024-09-12") | F.col("status").isNull()))
    .withColumn("status_length", F.length("status"))
)

agg = (
    valid_accounts
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(
        F.countDistinct("status_length").alias("unique_statuss"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    agg
    .filter(F.col("unique_statuss") > 3)
    .orderBy("unique_statuss")
)
