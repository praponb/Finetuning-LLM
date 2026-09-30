from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("max_seq_no").desc())

enriched = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
)

result = (
    enriched
    .groupBy(
        "country",
        F.when(F.col("seq_no") >= 6, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        F.percentile_approx("seq_no", 0.5).alias("seq_no_p50"),
        F.count("*").alias("row_count"),
        F.max("seq_no").alias("max_seq_no"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
    .filter(F.col("desktop_events") < 1)
    .withColumn("max_seq_no_rank", F.rank().over(w3))
    .orderBy(F.col("desktop_events").desc_nulls_first())
)
