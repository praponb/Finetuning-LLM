from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("is_tablet", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select(
        "*",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") < 370), "left_anti")
)

cleaned = (
    eligible
    .filter((F.col("started_at") > F.to_date(F.lit("2023-02-25"))) & F.col("country").isNotNull())
    .select(
        "*",
        (F.col("row_num") - F.col("is_tablet")).alias("row_num_minus_is_tablet"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.country") == "US")
    .select(
        "a.id",
        "a.customer_id",
        "a.browser",
        "a.is_tablet",
        F.col("p.credit_limit"),
        F.col("p.city"),
        F.col("p.email"),
    )
)

kept_keys = (
    enriched
    .select("customer_id", "browser")
    .intersect(
        df_sessions
        .filter(F.col("device") != "tablet")
        .select("customer_id", "browser")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

eligible_sessions = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (~F.col("c.page").isin(["/checkout", "/product"])), "left_semi")
)

by_type = (
    eligible_sessions
    .groupBy("customer_id", "browser")
    .pivot("city", ["Bangkok", "Berlin"])
    .agg(F.max("credit_limit"))
    .select(
        "customer_id",
        "browser",
        F.col("Bangkok").alias("bangkok"),
        F.col("Berlin").alias("berlin"),
    )
)

result = (
    by_type
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "browser", "bangkok", F.col("avg_subscription_monthly_fee"))
)
