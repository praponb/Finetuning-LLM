from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("browser", "device").orderBy("started_at", "id")

scored = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.size(F.collect_set("country")).alias("num_countries"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("seq_no")).otherwise(0)).alias("chrome_seq_no"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("num_countries").desc_nulls_last())
)
