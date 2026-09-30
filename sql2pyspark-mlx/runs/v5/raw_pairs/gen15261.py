from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w_hist = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_web_events
    .select(
        "*",
        F.first("page", ignorenulls=True).over(w_grp).alias("first_known_page"),
        F.sum("duration_sec").over(w_grp_2).alias("cumulative_duration_sec"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_90d"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("duration_sec").over(w_hist).alias("duration_sec_avg_5"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "event_time",
        "first_known_page",
        "cumulative_duration_sec",
        "duration_sec_count_90d",
        "row_seq",
        "duration_sec_avg_5",
        F.when(F.col("event_type").rlike("^(purchase|click)$"), "CORE").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("duration_sec_avg_5").desc_nulls_last())
)
