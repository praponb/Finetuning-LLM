from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-28").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date_plus_months")), "left_semi")
)

base = (
    not_flagged
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-03-20"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "opened_date_plus_months",
        F.when(F.col("status").isin(["OPEN", "CLOSED"]), "Core").otherwise("Other").alias("status_group"),
    )
)

remaining_keys = (
    base
    .select("status", "id")
    .exceptAll(
        df_accounts
        .filter(F.col("opened_date") < "2025-05-01")
        .select("status", "id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date_plus_months"))
    .groupingSets(
        [["status", "activity_month"], ["status"], []],
        "status",
        "activity_month",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max("balance").alias("max_balance"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
)

result = (
    totals
    .select(
        "status",
        "activity_month",
        "num_status",
        "max_balance",
        "row_count",
        F.when(F.col("status").rlike("^(CLOSED|OPEN)$"), "PRIORITY").when(F.col("status").rlike("^(FROZEN)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
)
