from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

enriched = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
)

matched_keys = (
    enriched
    .select("country", "customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("browser").isin(["Chrome", "Firefox"]))
        .select("country", "customer_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type") != "page_view"), "left_anti")
)

kept_keys = (
    eligible
    .select("device", "id")
    .exceptAll(
        df_sessions
        .filter(~F.col("country").isin(["UK", "TH"]))
        .select("device", "id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
)

result = (
    totals
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "desktop_count", F.col("sum_account_balance"))
    .orderBy(F.col("customer_id").desc())
)
