from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("event_type", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w_ordered = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "search"]))
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.max("duration_sec").over(w).alias("duration_sec_max_90d"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.lead("duration_sec", 1).over(w4).alias("next_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w_ordered).alias("last_known_duration_sec"),
    )
)
