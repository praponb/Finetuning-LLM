from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2024-04-12 01:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .withColumn("started_at_day", F.col("started_at").cast("date"))
    .withColumn("clean_browser", F.trim("browser"))
)

kept_keys = (
    valid
    .select("device")
    .exceptAll(
        df_sessions
        .filter(F.col("device").isin(["mobile", "desktop"]))
        .select("device")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.city"), F.col("p.signup_date"), F.col("p.email"))
)

stats = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .cube("activity_month", "country", "device")
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.countDistinct("device").alias("distinct_devices"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows_2"),
        F.grouping("activity_month").alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "country",
        "last_started_at",
        "firefox_rows",
        "distinct_devices",
        "num_browsers",
        "firefox_rows_2",
        "grouping_id",
    )
    .orderBy(F.col("grouping_id").desc_nulls_first())
)
