from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select(
        "*",
        F.sum("duration_sec").over(w).alias("page_sum_duration_sec"),
        F.row_number().over(w2).alias("event_seq"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_30d"),
    )
)
