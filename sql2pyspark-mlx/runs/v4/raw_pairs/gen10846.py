from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

cleaned = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country") == "JP", 1).otherwise(0).alias("is_jp"),
        F.concat("device", F.lit("/"), "country").alias("device_country"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

cleaned_sessions = (
    cleaned
    .filter(F.col("browser") == "Chrome")
    .withColumn("started_at_day", F.col("started_at").cast("date"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("N/A")))
)

kept = (
    cleaned_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < "2023-04-30"), "left_anti")
)

result = (
    kept
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "is_jp",
        "browser_clean",
        "started_at_day",
        F.when(F.col("browser").rlike("^(Firefox|Safari)$"), "CORE").otherwise("PRIORITY").alias("browser_zone"),
        F.col("sum_account_balance"),
    )
)
