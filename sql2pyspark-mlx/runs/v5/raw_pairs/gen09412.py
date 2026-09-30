from pyspark.sql import functions as F

rollup_totals = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.balance")).alias("avg_balance"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    rollup_totals
    .select(
        "status",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "EXTENDED").when(F.col("status").rlike("^(OPEN)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
    )
)
