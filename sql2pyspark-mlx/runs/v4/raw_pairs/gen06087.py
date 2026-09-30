from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") | (F.col("status") == "FROZEN"))
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "OPEN"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
