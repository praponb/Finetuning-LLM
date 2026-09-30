from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(
        F.when(F.col("a.account_type").isin(["CHECKING", "CREDIT"]), "group_a").otherwise("other").alias("account_type_group"),
    )
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
)
