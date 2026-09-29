from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "started_at",
        "country",
        F.substring("country", 1, 3).alias("country_prefix"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["quarter_start", "session_id_tier", "week_start"], ["quarter_start", "session_id_tier"], []],
        "quarter_start",
        "session_id_tier",
        "week_start",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("country").alias("distinct_countries"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.percentile_approx("session_id", 0.5).alias("session_id_p50"),
        F.countDistinct("country").alias("distinct_countries_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("session_id_tier").desc_nulls_last())
)
