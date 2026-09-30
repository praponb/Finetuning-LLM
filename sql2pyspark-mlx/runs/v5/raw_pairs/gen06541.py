from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-22 02:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.trim("country").alias("clean_country"),
    )
)

valid = (
    base
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "browser",
        "country",
        F.when(F.col("browser").isin(["Edge", "Firefox"]), "Priority").otherwise("Other").alias("browser_group"),
        F.trim("country").alias("clean_country_2"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

base_sessions = (
    valid
    .filter(F.col("country").isin(["DE", "TH"]))
    .select(
        "id",
        "browser",
        "country",
        "browser_group",
        "country_filled",
        F.upper("country").alias("country_upper"),
    )
)

latest_match = (
    base_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"), F.col("c.event_type"))
)

result = (
    latest_match
    .rollup("country", "event_type")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.sort_array(F.collect_set("country")).alias("country_list_2"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .orderBy(F.col("country").desc_nulls_last())
)
