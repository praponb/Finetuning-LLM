from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

latest = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.when(F.col("event_type").rlike("^(purchase|add_to_cart)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("session_id").desc_nulls_first())
)
