from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2024-01-01 06:00:00"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("country").isin(["JP", "SG"]), "Core").otherwise("Other").alias("country_group"),
    )
)

remaining_keys = (
    cleaned
    .select("country")
    .subtract(
        df_sessions
        .filter(F.hour("started_at").between(9, 17))
        .select("country")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

totals = (
    eligible
    .groupBy(
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
    )
    .agg(
        F.max("started_at").alias("last_seen"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min_2"),
    )
    .filter(F.col("duration_min") > 0)
)

result = (
    totals
    .select(
        "year_start",
        "quarter_start",
        "duration_min",
        "browser_list",
        "duration_min_2",
    )
    .orderBy(F.col("duration_min_2").desc(), F.col("quarter_start").desc())
)
