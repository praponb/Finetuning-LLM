from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec").between(196, 258)), "left_semi")
)

result = (
    not_flagged
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").rlike("^(Edge|Firefox)$"), "PRIORITY").when(F.col("browser").rlike("^(Chrome|Safari)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("browser_zone"),
        F.col("max_account_balance"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
