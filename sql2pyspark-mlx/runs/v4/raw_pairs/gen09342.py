from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

with_windows = (
    df_sessions
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country_2"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy("customer_id", "browser")
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
)
