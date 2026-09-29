from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("event_type").orderBy("duration_sec")
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

enriched = (
    df_web_events
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("duration_sec_dense_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "row_seq",
        "duration_sec_bucket",
        F.when(F.col("event_type").rlike("^(purchase|page_view)$"), "GROUP_A").when(F.col("event_type").rlike("^(click|add_to_cart)$"), "PRIORITY").otherwise("GROUP_B").alias("event_type_zone"),
    )
)
