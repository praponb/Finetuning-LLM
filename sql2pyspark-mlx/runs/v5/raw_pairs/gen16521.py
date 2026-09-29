from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("jp_count").desc())

matched_keys = (
    df_sessions
    .select("device", "id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id") == 25)
        .select("device", "id")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"), "device")
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.max("started_at").alias("last_seen"),
    )
    .filter(F.col("chrome_events") < 1000)
    .withColumn("jp_count_dense_rank", F.dense_rank().over(w))
)
