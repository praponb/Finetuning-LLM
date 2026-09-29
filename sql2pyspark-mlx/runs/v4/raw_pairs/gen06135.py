from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec_whole", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")

filtered = (
    df_web_events
    .filter(F.col("session_id").isin([1, 4, 29]))
    .withColumn("event_type_page_key", F.concat_ws(",", "event_type", "page"))
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
)

filtered_web_events = (
    filtered
    .filter((F.col("event_time") >= F.to_date(F.lit("2024-03-26"))) & F.col("page").isNotNull())
    .withColumn("event_type_lower", F.lower("event_type"))
)

enriched = (
    filtered_web_events
    .select(
        "*",
        F.avg("duration_sec_whole").over(w).alias("duration_sec_whole_avg_5"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.browser"))
)

result = (
    joined
    .groupBy("session_id")
    .agg(
        F.count_if(F.col("page") == "/product").alias("product_events"),
        F.round(F.sum("duration_sec_whole") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_whole_per_cart"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .orderBy("product_events")
)
