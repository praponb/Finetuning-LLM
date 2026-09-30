from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time_day").desc(), "id")
w2 = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("event_time_day").desc(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

result = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
