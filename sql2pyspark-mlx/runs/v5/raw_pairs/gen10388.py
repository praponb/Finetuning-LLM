from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("is_active_flag").desc(), "id")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("credit_limit", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("is_active_flag_tier").orderBy(F.col("last_known_age_per_de").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-07-16"))) & (~F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_clean"),
    )
)

enriched = (
    cleaned
    .filter(F.col("country") == "UK")
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("running_is_active_flag", F.sum("is_active_flag").over(w2))
    .withColumn("last_known_age", F.last("age", ignorenulls=True).over(w_grp))
    .withColumn("seq_no", F.row_number().over(w4))
)

with_last = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"), F.col("c.country").alias("session_country"))
)

valid = (
    with_last
    .filter((F.col("signup_date") >= F.lit("2023-04-03").cast("date")) & F.col("session_country").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "is_active_flag",
        "segment_clean",
        "row_seq",
        "running_is_active_flag",
        "last_known_age",
        "seq_no",
        "device",
        "session_country",
        (F.col("seq_no") * F.col("row_seq")).alias("seq_no_times_row_seq"),
    )
)

daily = (
    valid
    .groupBy(
        F.when(F.col("is_active_flag") >= 83, "HIGH").otherwise("LOW").alias("is_active_flag_tier"),
        "segment",
    )
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.round(F.sum("last_known_age") / F.nullif(F.count(F.when(F.col("session_country") == "DE", 1)), F.lit(0)), 2).alias("last_known_age_per_de"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
    )
    .filter(F.col("last_known_age_per_de") >= 0)
    .withColumn("last_known_age_per_de_percent_rank", F.percent_rank().over(w6))
)

result = (
    daily
    .select(
        "is_active_flag_tier",
        "segment",
        "last_known_age_per_de",
        "desktop_events",
        "last_known_age_per_de_percent_rank",
    )
    .orderBy("segment")
)
