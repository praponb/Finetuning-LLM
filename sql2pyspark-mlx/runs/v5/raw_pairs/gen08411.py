from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_time_day", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")
w5 = Window.partitionBy("event_type", "page").orderBy("seq_no", "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-08-14").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "event_type",
        "page",
        F.last("page", ignorenulls=True).over(w).alias("last_known_page"),
        F.last("page", ignorenulls=True).over(w2).alias("last_known_page_2"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.row_number().over(w4).alias("seq_no_3"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter(F.col("page").isNotNull())
    .withColumn("seq_no_minus_seq_no_3", (F.col("seq_no") - F.col("seq_no_3")))
)

result = (
    base
    .select(
        "id",
        "event_type",
        "last_known_page",
        "last_known_page_2",
        "seq_no_2",
        "rn",
        "seq_no_minus_seq_no_3",
        F.when(F.col("event_type").rlike("^(click|page_view)$"), "EXTENDED").otherwise("PRIORITY").alias("event_type_zone"),
    )
)
