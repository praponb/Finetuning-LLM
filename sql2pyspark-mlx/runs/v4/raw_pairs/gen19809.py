from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").asc_nulls_first(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

ranked = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

attributed = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched_keys = (
    attributed
    .select("browser", "country")
    .subtract(
        df_sessions
        .filter(F.col("browser") != "Safari")
        .select("browser", "country")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("started_at") > F.lit("2023-05-07 11:00:00").cast("timestamp")) & ~F.col("device").isin(["desktop", "mobile"]))
    .withColumn("country_plan", F.concat("country", F.lit(" "), "plan"))
)

result = (
    filtered
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "row_num",
        "plan",
        "country_plan",
        F.col("min_account_balance"),
    )
)
