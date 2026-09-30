from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", "s.device", "s.country", F.col("c.event_type"))
)

result = (
    latest_match
    .groupBy("event_type")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("th_events").asc_nulls_first())
)
