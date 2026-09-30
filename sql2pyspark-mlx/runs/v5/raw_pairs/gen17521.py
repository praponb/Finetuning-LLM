from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_sessions
    .filter(F.col("browser") != "Safari")
    .select(
        "id",
        "device",
        "country",
        F.row_number().over(w).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
    )
)

result = (
    scored
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD"))
    .rollup("event_seq_tier", "device")
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("device") == "mobile", F.col("event_seq"))).alias("mobile_total_event_seq"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("event_seq_per_tablet"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("event_seq_per_desktop"),
        F.grouping("event_seq_tier").alias("grouping_level"),
    )
    .filter(F.col("event_seq_per_desktop") >= 0)
    .orderBy(F.col("mobile_total_event_seq").desc_nulls_last())
)
