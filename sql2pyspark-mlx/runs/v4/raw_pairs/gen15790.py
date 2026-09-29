from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("device", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("device").isin(["tablet", "mobile", "desktop"]))
        .select("device", "customer_id")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("country")
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes_2"),
        F.min("started_at").alias("first_started_at"),
    )
)
