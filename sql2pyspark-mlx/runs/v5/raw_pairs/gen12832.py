from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("started_at", "id")

with_windows = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w4).alias("first_known_country_2"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        "browser",
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.max("started_at").alias("last_started_at"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("event_seq_per_jp"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
    )
    .orderBy(F.col("safari_rows").desc_nulls_first())
)
