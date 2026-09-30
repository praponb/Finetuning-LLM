from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/cart", "/checkout"]))
    .groupBy("event_type", "session_id")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("home_count") > 10)
    .select("event_type", "session_id", "home_count", "event_type_list")
    .orderBy("event_type", "session_id")
    .limit(20)
)
