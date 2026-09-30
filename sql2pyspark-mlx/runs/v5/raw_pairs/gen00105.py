from pyspark.sql import functions as F

rollup_totals = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .cube(
        F.col("p.account_type").alias("account_type"),
        F.col("a.merchant").alias("merchant"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.amount")), 2).alias("avg_amount"),
    )
)

result = (
    rollup_totals
    .select(
        "account_type",
        "merchant",
        "cnt",
        "avg_amount",
        F.when(F.col("merchant").rlike("^(Starbucks|Lazada)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
    .orderBy("merchant")
)
