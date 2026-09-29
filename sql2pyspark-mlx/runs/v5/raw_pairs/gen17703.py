from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").asc_nulls_first(), "id")

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("quarter", "started_at").alias("quarter_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.max("started_at").alias("last_started_at"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
