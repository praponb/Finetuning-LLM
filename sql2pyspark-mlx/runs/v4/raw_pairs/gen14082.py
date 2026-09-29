from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("duration_sec").asc_nulls_first())

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-04-21 13:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("page_filled", F.coalesce("page", F.lit("unassigned")))
)

result = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("duration_sec").over(w2).alias("cumulative_duration_sec"),
        F.percent_rank().over(w3).alias("duration_sec_percent_rank"),
    )
)
