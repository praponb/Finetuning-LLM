from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_web_events
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w_hist))
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("duration_sec_avg_3", F.avg("duration_sec").over(w2))
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "prev_duration_sec",
        "next_duration_sec",
        "seq_no",
        F.when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "CORE").otherwise("OTHER_ZONE").alias("event_type_zone"),
    )
    .orderBy(F.col("id").desc())
)
