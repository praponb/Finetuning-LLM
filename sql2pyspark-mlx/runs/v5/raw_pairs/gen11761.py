from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type", "page").orderBy("event_time", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/product"))) & (F.to_date("event_time") < "2023-03-09"))
    .select("*", F.concat("event_type", F.lit(", "), "page").alias("event_type_page"))
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "session_id", "event_type", "page", "event_time")
)

enriched = (
    deduped
    .filter(F.col("page").isin(["/search", "/checkout"]))
    .select(
        "id",
        "session_id",
        F.row_number().over(w2).alias("row_seq"),
        F.first("page", ignorenulls=True).over(w3).alias("first_known_page"),
        F.row_number().over(w4).alias("event_seq"),
        F.last("page", ignorenulls=True).over(w5).alias("last_known_page"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.first("row_seq", ignorenulls=True).over(w7).alias("first_known_row_seq"),
        F.avg("seq_no").over(w8).alias("seq_no_avg_3"),
    )
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "row_seq",
        "first_known_page",
        "event_seq",
        "seq_no",
        "first_known_row_seq",
        "seq_no_avg_3",
    )
    .orderBy(F.col("seq_no").asc_nulls_first())
)
