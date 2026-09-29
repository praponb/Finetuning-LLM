from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")

enriched = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w2))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

result = (
    enriched
    .rollup(
        F.date_trunc("WEEK", "started_at").alias("week_start"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.when(F.col("event_seq") >= 7, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq_2"),
    )
    .filter(F.col("avg_event_seq") < 0)
)
