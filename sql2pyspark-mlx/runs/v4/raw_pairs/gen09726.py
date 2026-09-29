from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
    .filter(F.col("balance_range") < 29)
    .select("status_group", "balance_range")
)
