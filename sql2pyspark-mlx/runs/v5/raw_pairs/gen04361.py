from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w3 = Window.orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")
w5 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device").orderBy("started_at", "id")
w7 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device").orderBy(F.col("seq_no").desc_nulls_last(), "id")
w9 = Window.partitionBy("country").orderBy("started_at", "id")
w10 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-06-25").cast("date"))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.concat("browser", F.lit(" - "), "country").alias("browser_country"),
        F.trim("device").alias("clean_device"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w5).alias("first_known_country"),
    )
)

with_prev = metrics.withColumn("prev_started_at", F.lag("started_at").over(w6))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "clean_device",
        "seq_no",
        "row_seq",
        "event_seq",
        F.sum("is_new_burst").over(w7).alias("session_no"),
    )
)

deduped = (
    sessions_sessions
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    deduped
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w9).isNull() | (F.col("started_at") > F.lag("started_at").over(w9) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "seq_no",
        "row_seq",
        "session_no",
        "row_num",
        F.sum("session_start_flag").over(w10).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("seq_no_per_th"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
    .orderBy(F.col("year_start").desc())
)
