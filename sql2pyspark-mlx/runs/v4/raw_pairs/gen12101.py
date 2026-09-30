from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.min("started_at").alias("started_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
)
